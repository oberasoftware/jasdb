package com.oberasoftware.jasdb.rest.service.loaders;

import com.oberasoftware.jasdb.api.session.DBInstance;
import com.oberasoftware.jasdb.api.engine.DBInstanceFactory;
import com.oberasoftware.jasdb.core.context.RequestContext;
import com.oberasoftware.jasdb.api.engine.EngineManager;
import com.oberasoftware.jasdb.api.exceptions.ConfigurationException;
import com.oberasoftware.jasdb.api.exceptions.JasDBStorageException;
import com.oberasoftware.jasdb.api.exceptions.RestException;
import com.oberasoftware.jasdb.rest.service.input.InputElement;
import com.oberasoftware.jasdb.rest.service.exceptions.SyntaxException;
import com.oberasoftware.jasdb.rest.service.input.OrderParam;
import com.oberasoftware.jasdb.rest.service.input.TokenType;
import com.oberasoftware.jasdb.rest.service.input.conditions.FieldCondition;
import com.oberasoftware.jasdb.rest.service.input.conditions.InputCondition;
import com.oberasoftware.jasdb.rest.model.InstanceCollection;
import com.oberasoftware.jasdb.rest.model.InstanceRest;
import com.oberasoftware.jasdb.rest.model.RestEntity;
import com.oberasoftware.jasdb.rest.model.serializers.RestResponseHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

@Component
public class InstanceModelLoader  extends AbstractModelLoader {
    private static final Logger LOG = LoggerFactory.getLogger(InstanceModelLoader.class);

	private static final String MODEL_NAME = "Instance";
    private static final String INSTANCES = "Instances";

    @Autowired
    private DBInstanceFactory instanceFactory;

    @Autowired
    private EngineManager engineManager;

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
        List<InstanceRest> instances = new ArrayList<>();
        for(DBInstance instance : instanceFactory.listInstances()) {
            instances.add(new InstanceRest(instance.getPath(), "OK", engineManager.getEngineVersion(), instance.getInstanceId()));
        }
        return instances;
    }
    
    private InstanceRest getInstance(String instanceId) throws RestException {
        try {
            DBInstance dbInstance = instanceFactory.getInstance(instanceId);
            InstanceRest instance = new InstanceRest(dbInstance.getPath(), "OK", engineManager.getEngineVersion(), dbInstance.getInstanceId());

            return instance;
        } catch(ConfigurationException e) {
            throw new RestException("Unable to retrieve the instance", e);
        }
    }

	@Override
	public RestEntity writeEntry(InputElement input, RestResponseHandler serializer, String rawData, RequestContext context) throws RestException {
        InstanceRest dbInstance = serializer.deserialize(InstanceRest.class, rawData);
        try {
            instanceFactory.addInstance(dbInstance.getInstanceId());

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
                LOG.debug("Receiving a instance delete operation for instance: {}", idCondition.getValue());
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
