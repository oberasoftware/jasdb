package nl.renarj.jasdb.rest.loaders;

import nl.renarj.core.utilities.StringUtils;
import nl.renarj.jasdb.api.context.RequestContext;
import nl.renarj.jasdb.api.metadata.Bag;
import nl.renarj.jasdb.api.model.DBInstance;
import nl.renarj.jasdb.api.model.DBInstanceFactory;
import nl.renarj.jasdb.core.SimpleKernel;
import nl.renarj.jasdb.core.exceptions.ConfigurationException;
import nl.renarj.jasdb.core.exceptions.JasDBStorageException;
import nl.renarj.jasdb.rest.exceptions.RestException;
import nl.renarj.jasdb.rest.exceptions.SyntaxException;
import nl.renarj.jasdb.rest.input.InputElement;
import nl.renarj.jasdb.rest.input.OrderParam;
import nl.renarj.jasdb.rest.input.TokenType;
import nl.renarj.jasdb.rest.input.conditions.FieldCondition;
import nl.renarj.jasdb.rest.input.conditions.InputCondition;
import nl.renarj.jasdb.rest.model.BagCollection;
import nl.renarj.jasdb.rest.model.ErrorEntity;
import nl.renarj.jasdb.rest.model.InstanceRest;
import nl.renarj.jasdb.rest.model.RestBag;
import nl.renarj.jasdb.rest.model.RestEntity;
import nl.renarj.jasdb.rest.serializers.RestResponseHandler;
import nl.renarj.jasdb.service.StorageService;
import nl.renarj.jasdb.service.StorageServiceFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.Response;
import java.util.ArrayList;
import java.util.List;

public class BagModelLoader extends AbstractModelLoader {
    private static final Logger log = LoggerFactory.getLogger(BagModelLoader.class);
    private static final String FLUSH_OPERATION = "flush";

    @Override
	public String[] getModelNames() {
		return new String[] {"Bag", "Bags"};
	}

    @Override
    public boolean isOperationSupported(String operation) {
        return FLUSH_OPERATION.equals(operation);
    }

    @Override
	public RestEntity loadModel(InputElement input, String begin, String top, List<OrderParam> orderParamList, RequestContext context) throws RestException {
		log.debug("Loading bag data for input: {}", input.getElementName());
		if(input.getPrevious() != null) {
			RestEntity previousEntity = input.getPrevious().getResult();
            if(previousEntity instanceof InstanceRest) {
                InstanceRest instance = (InstanceRest) previousEntity;
                return handleConditions(instance.getInstanceId(), input);
            } else {
                throw new RestException("Invalid syntax, can only retrieve bags for a specified instance");
            }
		} else {
            return handleConditions(null, input);
		}
	}
    
    private RestEntity handleConditions(String instanceId, InputElement input) throws RestException {
        try {
            DBInstanceFactory instanceFactory = SimpleKernel.getInstanceFactory();

            if(input.getCondition() != null) {
                return doSearch(instanceFactory.getInstance(instanceId), input);
            } else {
                return handleList(instanceFactory.getInstance(instanceId));
            }
        } catch(ConfigurationException e) {
            throw new RestException("Unable to retrieve DB instance", e);
        }
        
    }
	
	@Override
	public RestEntity writeEntry(InputElement input, RestResponseHandler serializer, String rawData, RequestContext context) throws RestException {
        if(input.getPrevious() != null) {
            RestEntity previousEntity = input.getPrevious().getResult();
            if(previousEntity instanceof InstanceRest) {
                InstanceRest instance = (InstanceRest) previousEntity;
                return createBag(instance.getInstanceId(), serializer, rawData);
            } else {
                throw new RestException("Invalid syntax, can only create bags for a specified instance");
            }
        } else {
            try {
                DBInstanceFactory instanceFactory = SimpleKernel.getInstanceFactory();
                return createBag(instanceFactory.getInstance().getInstanceId(), serializer, rawData);
            } catch(ConfigurationException e) {
                throw new RestException("Unable to load instance data", e);
            }
        }
	}

    @Override
    public RestEntity removeEntry(InputElement input, RestResponseHandler serializer, String rawData, RequestContext context) throws RestException {
        if(input.getPrevious() != null) {
            RestEntity previousEntity = input.getPrevious().getResult();
            if(previousEntity instanceof InstanceRest) {
                InstanceRest instance = (InstanceRest) previousEntity;
                removeBag(instance.getInstanceId(), input);
            } else {
                throw new RestException("Invalid syntax, can only remove bags for a specified instance");
            }
        } else {
            removeBag(null, input);
        }
        return null;
    }

    private void removeBag(String instance, InputElement input) throws RestException {
        InputCondition condition = input.getCondition();
        if(condition.getTokenType() == TokenType.LITERAL && ((FieldCondition)condition).getField().equals(FieldCondition.ID_PARAM)) {
            FieldCondition idCondition = (FieldCondition) condition;
            try {
                DBInstanceFactory instanceFactory = SimpleKernel.getInstanceFactory();

                DBInstance dbInstance;
                if(StringUtils.stringEmpty(instance)) {
                    dbInstance = instanceFactory.getInstance();
                } else {
                    dbInstance = instanceFactory.getInstance(instance);
                }

                SimpleKernel.getStorageServiceFactory().removeStorageService(dbInstance.getInstanceId(), idCondition.getValue());
            } catch(ConfigurationException e) {
                throw new RestException("Unable to load instance data", e);
            } catch(JasDBStorageException e) {
                throw new RestException("Unable to remove bag: " + e.getMessage());
            }
        } else {
            throw new RestException("Unable to remove bag, no name was specified");
        }
    }

    private RestEntity createBag(String instance, RestResponseHandler serializer, String rawData) throws RestException {
        RestBag bagData = serializer.deserialize(RestBag.class, rawData);

        try {
            if(StringUtils.stringNotEmpty(bagData.getName())) {
                StorageServiceFactory storageServiceFactory = SimpleKernel.getStorageServiceFactory();
                StorageService storageService = storageServiceFactory.getOrCreateStorageService(instance, bagData.getName());

                return new RestBag(instance, bagData.getName(), storageService.getSize(), storageService.getDiskSize());
            } else {
                throw new RestException("Cannot create bag, no name specified");
            }
        } catch(ConfigurationException e) {
            throw new RestException("Unable to create bag, unable to load kernel", e);
        } catch(JasDBStorageException e) {
            throw new RestException("Unable to create bag", e);
        }
    }

    public RestEntity doSearch(DBInstance instance, InputElement element) throws RestException {
		InputCondition inputCondition = element.getCondition();
		log.debug("Loading bag information based on input condition: {}", inputCondition);
		if(inputCondition instanceof FieldCondition) {
			FieldCondition fieldCondition = (FieldCondition) inputCondition;
            String bagName = fieldCondition.getValue();

			try {
                StorageServiceFactory storageServiceFactory = SimpleKernel.getStorageServiceFactory();
                StorageService storageService = storageServiceFactory.getStorageService(instance.getInstanceId(), bagName);
				if(storageService != null) {
					log.debug("Found a bag with name: {}", bagName);
					return new RestBag(instance.getInstanceId(), bagName, storageService.getSize(), storageService.getDiskSize());
				} else {
					return new ErrorEntity(Response.Status.NOT_FOUND.getStatusCode(), "No bag was found with name: " + fieldCondition.getValue());
				}
			} catch(JasDBStorageException e) {
				throw new RestException("Unable to load bag metadata", e);
			}
		} else {
			throw new SyntaxException("No recognized criteria specified, use parameter name=value or simple value");
		}
	}
	
	public RestEntity handleList(DBInstance instance) throws RestException {
		log.debug("Retrieving full list of bags on storage instance: {}", instance.getInstanceId());
		List<RestBag> bags = new ArrayList<RestBag>();
		try {
            StorageServiceFactory storageServiceFactory = SimpleKernel.getStorageServiceFactory();
			for(Bag bag : instance.getBags()) {
                StorageService storageService = storageServiceFactory.getStorageService(instance.getInstanceId(), bag.getName());
				bags.add(new RestBag(instance.getInstanceId(), bag.getName(), storageService.getSize(), storageService.getDiskSize()));
			}
		} catch(JasDBStorageException e) {
			throw new RestException("Unable to load bags", e);
		}
		
		return new BagCollection(bags);
	}

    @Override
    public RestEntity doOperation(InputElement input) throws RestException {
        String operation = input.getElementName();
        InputElement previous = input.getPrevious();
        if(previous != null && previous.getResult() != null) {
            RestEntity entity = previous.getResult();
            if(entity instanceof RestBag && FLUSH_OPERATION.equals(operation)) {
                RestBag bag = (RestBag) entity;

                try {
                    StorageServiceFactory storageServiceFactory = SimpleKernel.getStorageServiceFactory();
                    StorageService storageService = storageServiceFactory.getOrCreateStorageService(bag.getInstanceId(), bag.getName());
                    storageService.flush();

                    return bag;
                } catch(JasDBStorageException e) {
                    throw new RestException("Unable to flush bag: " + bag.getName(), e);
                }
            }
        }

        throw new RestException("Unable to perform operation: " + operation + " on bag");
    }
}
