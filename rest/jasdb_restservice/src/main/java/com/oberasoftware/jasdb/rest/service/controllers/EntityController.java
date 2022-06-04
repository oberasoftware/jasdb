package com.oberasoftware.jasdb.rest.service.controllers;

import com.oberasoftware.jasdb.api.engine.DBInstanceFactory;
import com.oberasoftware.jasdb.api.exceptions.JasDBException;
import com.oberasoftware.jasdb.api.exceptions.JasDBStorageException;
import com.oberasoftware.jasdb.api.exceptions.RestException;
import com.oberasoftware.jasdb.api.index.query.SearchLimit;
import com.oberasoftware.jasdb.api.session.DBInstance;
import com.oberasoftware.jasdb.api.session.Entity;
import com.oberasoftware.jasdb.api.session.query.BlockType;
import com.oberasoftware.jasdb.api.session.query.Order;
import com.oberasoftware.jasdb.api.session.query.QueryBuilder;
import com.oberasoftware.jasdb.api.session.query.QueryResult;
import com.oberasoftware.jasdb.core.context.RequestContext;
import com.oberasoftware.jasdb.engine.StorageService;
import com.oberasoftware.jasdb.engine.StorageServiceFactory;
import com.oberasoftware.jasdb.engine.query.BuilderTransformer;
import com.oberasoftware.jasdb.rest.model.ErrorEntity;
import com.oberasoftware.jasdb.rest.model.RestEntity;
import com.oberasoftware.jasdb.rest.model.serializers.json.entity.EntityHandler;
import com.oberasoftware.jasdb.rest.model.streaming.StreamableEntityCollection;
import com.oberasoftware.jasdb.rest.model.streaming.StreamedEntity;
import com.oberasoftware.jasdb.api.exceptions.SyntaxException;
import com.oberasoftware.jasdb.rest.service.input.*;
import com.oberasoftware.jasdb.rest.service.input.conditions.AndBlockOperation;
import com.oberasoftware.jasdb.rest.service.input.conditions.BlockOperation;
import com.oberasoftware.jasdb.rest.service.input.conditions.FieldCondition;
import com.oberasoftware.jasdb.rest.service.input.conditions.InputCondition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.List;

import static com.oberasoftware.jasdb.core.utils.StringUtils.stringNotEmpty;
import static com.oberasoftware.jasdb.rest.service.input.OrderParameterParsing.getOrderParams;
import static com.oberasoftware.jasdb.rest.service.controllers.ControllerUtil.getRequestContext;
import static org.springframework.web.bind.annotation.RequestMethod.*;

@RestController
public class EntityController {
    private static final Logger LOG = LoggerFactory.getLogger(EntityController.class);

    private static final EntityHandler ENTITY_HANDLER = new EntityHandler();

    private enum OPERATION_TYPE {
        UPDATE,
        INSERT
    }

    private final DBInstanceFactory dbInstanceFactory;

    private final StorageServiceFactory storageServiceFactory;

    @Autowired
    public EntityController(StorageServiceFactory storageServiceFactory, DBInstanceFactory dbInstanceFactory) {
        this.storageServiceFactory = storageServiceFactory;
        this.dbInstanceFactory = dbInstanceFactory;
    }

    @RequestMapping(value = "/Instances({instanceId})/Bags({bagName})/Entities", method = GET)
    public void getEntities(@PathVariable String instanceId, @PathVariable String bagName,
                            @RequestParam(required = false, defaultValue = "-1") int top,
                            HttpServletRequest request, HttpServletResponse response) throws JasDBStorageException {
        LOG.debug("Entity listing request instance/bag {}/{} top: {}", instanceId, bagName, top);
        OutputHandler.createResponse(handleCollection(request, instanceId, bagName, top), response);
    }

    @RequestMapping(value = "/Bags({bagName})/Entities", method = GET)
    public void getEntities(@PathVariable String bagName, @RequestParam(required = false, defaultValue = "-1") int top,
                            HttpServletRequest request, HttpServletResponse response) throws JasDBException {
        String instanceId = ControllerUtil.getInstance(dbInstanceFactory, null).getInstanceId();
        LOG.debug("Entity listing request instance/bag {}/{} top: {}", instanceId, bagName, top);

        OutputHandler.createResponse(handleCollection(request, instanceId, bagName, top), response);
    }

    @RequestMapping(value = "/Instances({instanceId})/Bags({bagName})/Entities({entityQuery:.*})", method = GET)
    public void queryEntities(@PathVariable String instanceId, @PathVariable String bagName,
                              @RequestParam(required = false, defaultValue = "0") int begin,
                              @RequestParam(required = false, defaultValue = "-1") int top,
                              @RequestParam(required = false, defaultValue = "") String orderBy,
                              @PathVariable String entityQuery, HttpServletRequest request, HttpServletResponse response) throws JasDBException, UnsupportedEncodingException {
        String q = URLDecoder.decode(entityQuery, "UTF8");
        LOG.debug("Received query: {} begin: {} top: {} orderBy: {} for instance/bag: {}/{}", q, begin, top, orderBy, instanceId, bagName);
        InputCondition inputCondition = new InputParser(new InputScanner(q)).getCondition();
        if(inputCondition == null) {
            inputCondition = new AndBlockOperation();
        }
        StorageService storageService = storageServiceFactory.getStorageService(instanceId, bagName);
        RequestContext context = getRequestContext(request);
        List<OrderParam> orderParamList = getOrderParams(orderBy);

        RestEntity entity = handleQuery(storageService, inputCondition, begin, top, orderParamList, context);
        OutputHandler.createResponse(entity, response);

    }

    @RequestMapping(value = "/Bags({bagName})/Entities({entityQuery:.*})", method = GET)
    public void queryEntities(@PathVariable String bagName, @PathVariable String entityQuery,
                              @RequestParam(required = false, defaultValue = "0") int begin,
                              @RequestParam(required = false, defaultValue = "-1") int max,
                              @RequestParam(required = false, defaultValue = "") String orderBy,
                              HttpServletRequest request, HttpServletResponse response) throws JasDBException, UnsupportedEncodingException {
        DBInstance instance = dbInstanceFactory.getInstance();
        queryEntities(instance.getInstanceId(), bagName, begin, max, orderBy, entityQuery, request, response);
    }

    @RequestMapping(value = "/Instances({instanceId})/Bags({bagName})/Entities", consumes = "application/json",
            produces = "application/json", method = POST)
	public void writeEntry(@PathVariable String instanceId, @PathVariable String bagName,
                                 HttpServletRequest request, HttpServletResponse response, @RequestBody String rawData) throws RestException {
        RestEntity entity = doModificationOperation(instanceId, bagName, rawData, getRequestContext(request), OPERATION_TYPE.INSERT);
        OutputHandler.createResponse(entity, response);
	}

    @RequestMapping(value = "/Instances({instanceId})/Bags({bagName})/Entities", consumes = "application/json",
            produces = "application/json", method = PUT)
    public void updateEntry(@PathVariable String instanceId, @PathVariable String bagName,
                                  HttpServletRequest request, HttpServletResponse response, @RequestBody String rawData) throws RestException {
        RestEntity entity = doModificationOperation(instanceId, bagName, rawData, getRequestContext(request), OPERATION_TYPE.UPDATE);
        OutputHandler.createResponse(entity, response);
    }

    @RequestMapping(value = "/Instances({instanceId})/Bags({bagName})/Entities({entityId})", produces = "application/json", method = DELETE)
    public RestEntity removeEntry(@PathVariable String instanceId, @PathVariable String bagName, @PathVariable String entityId,
                                  HttpServletRequest request) throws RestException {
        if(stringNotEmpty(instanceId) && stringNotEmpty(bagName) && stringNotEmpty(entityId)) {
            try {
                LOG.debug("Doing remove of entity with id: {}", entityId);
                StorageService storageService = storageServiceFactory.getOrCreateStorageService(instanceId, bagName);

                storageService.removeEntity(getRequestContext(request), entityId);
                return null;
            } catch(JasDBStorageException e) {
                throw new RestException("Unable to remove entity: " + e.getMessage());
            }
        } else {
            throw new SyntaxException("Cannot remove entity, invalid parameters for delete operation");
        }
    }

    private RestEntity doModificationOperation(String instanceId, String bagName, String rawData, RequestContext context, OPERATION_TYPE type) throws RestException {
        if(stringNotEmpty(instanceId) && stringNotEmpty(bagName)) {

            LOG.debug("Raw entity data received: {}", rawData);

            StreamedEntity streamedEntity = ENTITY_HANDLER.deserialize(StreamedEntity.class, rawData);
            Entity storeEntity = streamedEntity.getEntity();

            try {
                StorageService storageService = storageServiceFactory.getOrCreateStorageService(instanceId, bagName);
                if(type == OPERATION_TYPE.UPDATE) {
                    LOG.debug("Updating entity with id: {}", storeEntity.getInternalId());
                    storageService.persistEntity(context, storeEntity);
                } else if(type == OPERATION_TYPE.INSERT) {
                    LOG.debug("Inserting new entity into bag: {}", bagName);

                    storageService.insertEntity(context, storeEntity);
                }
                return new StreamedEntity(storeEntity);
            } catch(JasDBStorageException e) {
                throw new RestException("Unable to store entity: " + e.getMessage());
            }
        } else {
            throw new SyntaxException("Cannot store entity, no Bag specified");
        }
    }

	private RestEntity handleQuery(StorageService storageService, InputCondition condition, int begin, int top, List<OrderParam> orderParams, RequestContext context) throws JasDBStorageException {
        LOG.debug("Query: {} begin: {} top: {}", condition, begin, top);
        if(condition.getTokenType() == TokenType.LITERAL && ((FieldCondition)condition).getField().equals(FieldCondition.ID_PARAM)) {
            FieldCondition idCondition = (FieldCondition) condition;
            return requestById(storageService, idCondition.getValue(), context);
        } else {
            QueryBuilder parentBuilder = generateQueryBuilder(condition, QueryBuilder.createBuilder());
            setOrderingParams(parentBuilder, orderParams);
            long start = System.currentTimeMillis();
            QueryResult result = storageService.search(context, BuilderTransformer.transformBuilder(parentBuilder), new SearchLimit(begin, top), parentBuilder.getSortParams());
            long end = System.currentTimeMillis();

            StreamableEntityCollection collection = new StreamableEntityCollection(result);
            collection.setTimeMilliseconds((end - start));

            return collection;
        }
	}

    private RestEntity requestById(StorageService storageService, String requestedId, RequestContext context) throws JasDBStorageException {
        Entity entity = storageService.getEntityById(context, requestedId);
        if(entity != null) {
            return new StreamedEntity(entity);
        } else {
            return new ErrorEntity(404, "No entity was found with id: " + requestedId);
        }
    }

    private void setOrderingParams(QueryBuilder queryBuilder, List<OrderParam> orderParams) {
        if(!orderParams.isEmpty()) {
            for(OrderParam orderParam : orderParams) {
                Order order = orderParam.getSortDirection() == OrderParam.DIRECTION.ASC ? Order.ASCENDING : Order.DESCENDING;
                queryBuilder.sortBy(orderParam.getField(), order);
            }
        }
    }

	private QueryBuilder generateQueryBuilder(InputCondition currentCondition, QueryBuilder parentBuilder) {
		if(currentCondition.getTokenType() == TokenType.LITERAL) {
			FieldCondition fieldCondition = (FieldCondition) currentCondition;
			parentBuilder.field(fieldCondition.getField()).operation(fieldCondition.getOperator().getQueryOperator(), fieldCondition.getValue());
		} else if(currentCondition.getTokenType() == TokenType.AND || currentCondition.getTokenType() == TokenType.OR){
			BlockOperation blockOperation = (BlockOperation) currentCondition;

			QueryBuilder builder = QueryBuilder.createBuilder(currentCondition.getTokenType() == TokenType.AND ? BlockType.AND : BlockType.OR);
			for(InputCondition childCondition : blockOperation.getChildConditions()) {
				generateQueryBuilder(childCondition, builder);
			}
			parentBuilder.addQueryBlock(builder);
		}

		return parentBuilder;
	}

	private StreamableEntityCollection handleCollection(HttpServletRequest request, String instanceId, String bagName, int max) throws JasDBStorageException {
        StorageService storageService = storageServiceFactory.getOrCreateStorageService(instanceId, bagName);

		long start = System.currentTimeMillis();
		QueryResult queryResult;
		if(max != -1) {
			queryResult = storageService.getEntities(getRequestContext(request), max);
		} else {
			queryResult = storageService.getEntities(getRequestContext(request));
		}

        StreamableEntityCollection collection = new StreamableEntityCollection(queryResult);
		long end = System.currentTimeMillis();
		collection.setTimeMilliseconds((end - start));

		return collection;
	}
}
