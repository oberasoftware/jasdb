package com.oberasoftware.jasdb.rest.client;

import com.oberasoftware.jasdb.api.model.Instance;
import com.oberasoftware.jasdb.api.exceptions.ConfigurationException;
import com.oberasoftware.jasdb.api.model.NodeInformation;
import nl.renarj.jasdb.remote.InstanceConnector;
import nl.renarj.jasdb.remote.RemotingContext;
import nl.renarj.jasdb.remote.exceptions.RemoteException;
import com.oberasoftware.jasdb.api.exceptions.RestException;
import com.oberasoftware.jasdb.rest.model.InstanceCollection;
import com.oberasoftware.jasdb.rest.model.InstanceRest;
import com.oberasoftware.jasdb.rest.model.serializers.json.JsonRestResponseHandler;

import java.io.ByteArrayOutputStream;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * @author Renze de Vries
 */
public class InstanceRestConnector extends RemoteRestConnector implements InstanceConnector {
    public InstanceRestConnector(NodeInformation nodeInformation) throws ConfigurationException {
        super(nodeInformation);
    }

    @Override
    public Instance getInstance(RemotingContext context, String instanceId) throws RemoteException {
        String connectionString = new RestConnectionBuilder().instance(instanceId).getConnectionString();
        try {
            ClientResponse clientResponse = doRequest(context, connectionString);
            try {
                return new JsonRestResponseHandler().deserialize(InstanceRest.class, clientResponse.getEntityInputStream());
            } catch(RestException e) {
                throw new RemoteException("Unable to parse remote instance data", e);
            } finally {
                clientResponse.close();
            }
        } catch(ResourceNotFoundException e) {
            throw new RemoteException("Unable to find instance: " + instanceId);
        }
    }

    @Override
    public List<Instance> getInstances(RemotingContext context) throws RemoteException {
        String connectionString = new RestConnectionBuilder().instance().getConnectionString();
        try {
            ClientResponse clientResponse = doRequest(context, connectionString);
            try {
                InstanceCollection instanceCollection = new JsonRestResponseHandler().deserialize(InstanceCollection.class, clientResponse.getEntityInputStream());

                return new ArrayList<>(instanceCollection.getInstances());
            } catch(RestException e) {
                throw new RemoteException("Unable to parse remote instance data", e);
            } finally {
                clientResponse.close();
            }
        } catch(ResourceNotFoundException e) {
            throw new RemoteException("Unable to load instances", e);
        }
    }

    @Override
    public Instance addInstance(RemotingContext context, String instanceId) throws RemoteException {
        String connectionString = new RestConnectionBuilder().instance().getConnectionString();

        try {
            InstanceRest instance = new InstanceRest(null, "OK", null, instanceId);
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            new JsonRestResponseHandler().serialize(instance, bos);

            ClientResponse clientResponse = doRequest(context, connectionString, new HashMap<>(), bos.toString(CHARACTER_ENCODING), REQUEST_MODE.POST);
            try {
                instance = new JsonRestResponseHandler().deserialize(InstanceRest.class, clientResponse.getEntityInputStream());
            } finally {
                clientResponse.close();
            }

            return instance;
        } catch(RestException e) {
            throw new RemoteException("Unable to parse remote bag definition", e);
        } catch(UnsupportedEncodingException e) {
            throw new RemoteException("Unable to serialize bag", e);
        }
    }

    @Override
    public void removeInstance(RemotingContext context, String instanceId) throws RemoteException {
        String connectionString = new RestConnectionBuilder().instance(instanceId).getConnectionString();
        doRequest(context, connectionString, new HashMap<>(), null, REQUEST_MODE.DELETE);
    }
}
