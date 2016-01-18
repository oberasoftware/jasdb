package nl.renarj.jasdb.rest.loaders;

import nl.renarj.core.utilities.StringUtils;
import nl.renarj.jasdb.api.SimpleEntity;
import nl.renarj.jasdb.api.context.RequestContext;
import nl.renarj.jasdb.api.query.BlockType;
import nl.renarj.jasdb.api.query.Order;
import nl.renarj.jasdb.api.query.QueryBuilder;
import nl.renarj.jasdb.api.query.QueryResult;
import nl.renarj.jasdb.api.query.SortParameter;
import nl.renarj.jasdb.core.exceptions.JasDBStorageException;
import nl.renarj.jasdb.index.result.SearchLimit;
import nl.renarj.jasdb.rest.exceptions.RestException;
import nl.renarj.jasdb.rest.exceptions.SyntaxException;
import nl.renarj.jasdb.rest.input.InputElement;
import nl.renarj.jasdb.rest.input.OrderParam;
import nl.renarj.jasdb.rest.input.TokenType;
import nl.renarj.jasdb.rest.input.conditions.BlockOperation;
import nl.renarj.jasdb.rest.input.conditions.FieldCondition;
import nl.renarj.jasdb.rest.input.conditions.InputCondition;
import nl.renarj.jasdb.rest.model.ErrorEntity;
import nl.renarj.jasdb.rest.model.Partition;
import nl.renarj.jasdb.rest.model.RestBag;
import nl.renarj.jasdb.rest.model.RestEntity;
import nl.renarj.jasdb.rest.model.streaming.StreamableEntityCollection;
import nl.renarj.jasdb.rest.model.streaming.StreamedEntity;
import nl.renarj.jasdb.rest.serializers.RestResponseHandler;
import nl.renarj.jasdb.service.StorageService;
import nl.renarj.jasdb.service.StorageServiceFactory;
import nl.renarj.jasdb.storage.query.BuilderTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
import javax.ws.rs.core.Response;
import java.util.ArrayList;
import java.util.List;

@Component
public class EntityModelLoader extends AbstractModelLoader {
    private enum OPERATION_TYPE {
        UPDATE,
        INSERT
    }
	private static final Logger log = LoggerFactory.getLogger(EntityModelLoader.class);

    @Inject
    private StorageServiceFactory storageServiceFactory;
	
	@Override
	public String[] getModelNames() {
		return new String[] {"Entities"};
	}

    @Override
    public RestEntity loadModel(InputElement input, String start, String top, List<OrderParam> orderParamList, RequestContext context) throws RestException {
        InputElement previous = input.getPrevious();
        int max = getSafeInteger(top, -1);
        int begin = getSafeInteger(start, 0);

        try {
            if(previous != null && previous.getResult() instanceof RestBag) {
                RestBag bag = (RestBag) previous.getResult();

                return handleBagRelation(bag, input, begin, max, orderParamList, context);
            } else if(previous != null && previous.getResult() instanceof Partition) {
                Partition partition = (Partition) previous.getResult();
                return handlePartitionEntities(partition, context, begin, max);
            } else {
                throw new SyntaxException("Cannot retrieve entities, no Bag specified");
            }
        } catch(JasDBStorageException e) {
            throw new RestException("Unable to retrieve the entities", e);
        }

    }

    private RestEntity handleBagRelation(RestBag bag, InputElement input, int begin, int max, List<OrderParam> orderParams, RequestContext requestContext) throws JasDBStorageException {
        StorageService storageService = storageServiceFactory.getOrCreateStorageService(bag.getInstanceId(), bag.getName());

        if(input.getCondition() != null) {
            return handleQuery(storageService, input, begin, max, orderParams, requestContext);
        } else {
            return handleCollection(storageService, requestContext, max);
        }
    }

	@Override
	public RestEntity writeEntry(InputElement input, RestResponseHandler serializer, String rawData, RequestContext context) throws RestException {
        return doModificationOperation(input, serializer, rawData, context, OPERATION_TYPE.INSERT);
	}

    @Override
    public RestEntity removeEntry(InputElement input, RestResponseHandler serializer, String rawData, RequestContext context) throws RestException {
        InputElement previous = input.getPrevious();
        if(previous != null && previous.getResult() instanceof RestBag) {
            RestBag bag = (RestBag) previous.getResult();

            InputCondition condition = input.getCondition();
            if(condition.getTokenType() == TokenType.LITERAL && ((FieldCondition)condition).getField().equals(FieldCondition.ID_PARAM)) {
                FieldCondition idCondition = (FieldCondition) condition;

                try {
                    log.debug("Doing remove of entity with id: {}", idCondition.getValue());
                    StorageService storageService = storageServiceFactory.getOrCreateStorageService(bag.getInstanceId(), bag.getName());

                    storageService.removeEntity(context, idCondition.getValue());
                    return null;
                } catch(JasDBStorageException e) {
                    throw new RestException("Unable to remove entity: " + e.getMessage());
                }
            } else {
                throw new RestException("Unable to do remove operation, no id was specified");
            }
        } else {
            throw new SyntaxException("Cannot store entity, no Bag specified");
        }
    }

    @Override
    public RestEntity updateEntry(InputElement input, RestResponseHandler serializer, String rawData, RequestContext context) throws RestException {
        return doModificationOperation(input, serializer, rawData, context, OPERATION_TYPE.UPDATE);
    }

    private RestEntity doModificationOperation(InputElement input, RestResponseHandler serializer, String rawData, RequestContext context, OPERATION_TYPE type) throws RestException {
        InputElement previous = input.getPrevious();
        if(previous != null && previous.getResult() instanceof RestBag) {
            RestBag bag = (RestBag) previous.getResult();
            log.debug("Raw entity data received: {}", rawData);
            StreamedEntity streamedEntity = serializer.deserialize(StreamedEntity.class, rawData);
            SimpleEntity storeEntity = streamedEntity.getEntity();

            try {
                StorageService storageService = storageServiceFactory.getOrCreateStorageService(bag.getInstanceId(), bag.getName());

                if(type == OPERATION_TYPE.UPDATE) {
                    log.debug("Updating entity with id: {}", storeEntity.getInternalId());
                    storageService.updateEntity(context, storeEntity);
                } else if(type == OPERATION_TYPE.INSERT) {
                    log.debug("Inserting new entity into bag: {}", bag.getName());

                    storageService.persistEntity(context, storeEntity);
                }
                return new StreamedEntity(storeEntity);
            } catch(JasDBStorageException e) {
                throw new RestException("Unable to store entity: " + e.getMessage());
            }
        } else {
            throw new SyntaxException("Cannot store entity, no Bag specified");
        }
    }

    private int getSafeInteger(String top, int fallback) throws SyntaxException {
		if(StringUtils.stringNotEmpty(top)) {
			try {
				int parsedInt = Integer.parseInt(top);
				if(parsedInt > 0) {
					return parsedInt;
				} else {
					throw new SyntaxException("Integer should be higher than 0");
				}
			} catch(NumberFormatException e) {
				throw new SyntaxException("Invalid Integer specified: " + top);
			}
		} else {
			return fallback;
		}
	}

	private RestEntity handleQuery(StorageService storageService, InputElement element, int begin, int top, List<OrderParam> orderParams, RequestContext context) throws JasDBStorageException {
        InputCondition condition = element.getCondition();
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
        SimpleEntity entity = storageService.getEntityById(context, requestedId);
        if(entity != null) {
            return new StreamedEntity(entity);
        } else {
            return new ErrorEntity(Response.Status.NOT_FOUND.getStatusCode(), "No entity was found with id: " + requestedId);
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

	private StreamableEntityCollection handleCollection(StorageService storageService, RequestContext context, int max) throws JasDBStorageException {
		long start = System.currentTimeMillis();
		QueryResult queryResult;
		if(max != -1) {
			queryResult = storageService.getEntities(context, max);
		} else {
			queryResult = storageService.getEntities(context);
		}

        StreamableEntityCollection collection = new StreamableEntityCollection(queryResult);
		long end = System.currentTimeMillis();
		collection.setTimeMilliseconds((end - start));

		return collection;
	}
    
    private StreamableEntityCollection handlePartitionEntities(Partition partition, RequestContext context, int begin, int max) throws JasDBStorageException {
        StorageService storageService = storageServiceFactory.getOrCreateStorageService(partition.getInstance(), partition.getBag());

        log.debug("Retrieving entities for partition: {} begin: {} max: {}", partition.getPartitionId(), begin, max);

        QueryBuilder partitionQuery = QueryBuilder.createBuilder().field(StorageService.PARTITION_PROPERTY).value(partition.getPartitionId());
        long start = System.currentTimeMillis();
        QueryResult result = storageService.search(context, BuilderTransformer.transformBuilder(partitionQuery),
                new SearchLimit(begin, max), new ArrayList<SortParameter>());
        StreamableEntityCollection collection = new StreamableEntityCollection(result);
        long end = System.currentTimeMillis();
        collection.setTimeMilliseconds((end - start));

        return collection;
    }
}
