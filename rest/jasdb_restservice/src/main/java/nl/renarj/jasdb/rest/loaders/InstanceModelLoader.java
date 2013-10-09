package nl.renarj.jasdb.rest.loaders;

import nl.renarj.jasdb.api.context.RequestContext;
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
import nl.renarj.jasdb.rest.model.InstanceCollection;
import nl.renarj.jasdb.rest.model.InstanceRest;
import nl.renarj.jasdb.rest.model.RestEntity;
import nl.renarj.jasdb.rest.serializers.RestResponseHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class InstanceModelLoader  extends AbstractModelLoader {
    private static final Logger LOG = LoggerFactory.getLogger(InstanceModelLoader.class);

	private static final String MODEL_NAME = "Instance";
    private static final String INSTANCES = "Instances";

    public InstanceModelLoader() {
		
	}

	@Override
	public String[] getModelNames() {
		return new String[] { MODEL_NAME, INSTANCES };
	}

	@Override
	public RestEntity loadModel(InputElement input, String begin, String top, List<OrderParam> orderParamList, RequestContext context) throws RestException {
		if(input.getCondition() == null) {
            LOG.debug("Loading instances list");
            return new InstanceCollection(loadInstances());
		} else {
            InputCondition condition = input.getCondition();
            if(condition instanceof FieldCondition) {
                FieldCondition fieldCondition = (FieldCondition) condition;
                LOG.debug("Loading instance data for instance: {}", fieldCondition);

                InstanceRest instance = getInstance(fieldCondition.getValue());
                input.setResult(instance);

                return instance;
            } else {
			    throw new SyntaxException("Requesting instance data is not supported");
            }
		}
	}

    private List<InstanceRest> loadInstances() throws RestException {
        try {
            DBInstanceFactory instanceFactory = SimpleKernel.getInstanceFactory();
            List<InstanceRest> instances = new ArrayList<InstanceRest>();
            for(DBInstance instance : instanceFactory.listInstances()) {
                instances.add(new InstanceRest(instance.getPath(), "OK", SimpleKernel.getVersion(), instance.getInstanceId()));
            }
            return instances;
        } catch(ConfigurationException e) {
            throw new RestException("Unable to load instance list", e);
        }
    }
    
    private InstanceRest getInstance(String instanceId) throws RestException {
        try {
            DBInstanceFactory instanceFactory = SimpleKernel.getInstanceFactory();
            DBInstance dbInstance = instanceFactory.getInstance(instanceId);
            InstanceRest instance = new InstanceRest(dbInstance.getPath(), "OK", SimpleKernel.getVersion(), dbInstance.getInstanceId());

            return instance;
        } catch(ConfigurationException e) {
            throw new RestException("Unable to retrieve the instance", e);
        }
    }

	@Override
	public RestEntity writeEntry(InputElement input, RestResponseHandler serializer, String rawData, RequestContext context) throws RestException {
        InstanceRest dbInstance = serializer.deserialize(InstanceRest.class, rawData);
        try {
            DBInstanceFactory instanceFactory = SimpleKernel.getInstanceFactory();
            instanceFactory.addInstance(dbInstance.getInstanceId(), dbInstance.getPath());

            return getInstance(dbInstance.getInstanceId());
        } catch(JasDBStorageException e) {
            throw new RestException("Unable to create new instance: " + e.getMessage());
        }
	}

    @Override
    public RestEntity removeEntry(InputElement input, RestResponseHandler serializer, String rawData, RequestContext context) throws RestException {
        InputCondition condition = input.getCondition();
        if(condition.getTokenType() == TokenType.LITERAL && ((FieldCondition)condition).getField().equals(FieldCondition.ID_PARAM)) {
            FieldCondition idCondition = (FieldCondition) condition;

            try {
                DBInstanceFactory instanceFactory = SimpleKernel.getInstanceFactory();
                instanceFactory.deleteInstance(idCondition.getValue());

                return null;
            } catch(JasDBStorageException e) {
                throw new RestException("Unable to remove instance: " + idCondition.getValue());
            }
        } else {
            throw new RestException("Unable to remove instance, no instanceId specified");
        }
    }
}
