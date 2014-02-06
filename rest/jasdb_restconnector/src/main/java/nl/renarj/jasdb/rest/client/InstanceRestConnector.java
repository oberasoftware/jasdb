package nl.renarj.jasdb.rest.client;

import nl.renarj.jasdb.api.metadata.Instance;
import nl.renarj.jasdb.core.exceptions.ConfigurationException;
import nl.renarj.jasdb.core.locator.NodeInformation;
import nl.renarj.jasdb.remote.InstanceConnector;
import nl.renarj.jasdb.remote.RemotingContext;
import nl.renarj.jasdb.remote.exceptions.RemoteException;
import nl.renarj.jasdb.rest.exceptions.RestException;
import nl.renarj.jasdb.rest.model.InstanceCollection;
import nl.renarj.jasdb.rest.model.InstanceRest;
import nl.renarj.jasdb.rest.serializers.json.JsonRestResponseHandler;

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

                return new ArrayList<Instance>(instanceCollection.getInstances());
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
    public Instance addInstance(RemotingContext context, String instanceId, String instancePath) throws RemoteException {
        String connectionString = new RestConnectionBuilder().instance().getConnectionString();

        try {
            InstanceRest instance = new InstanceRest(instancePath, "OK", null, instanceId);
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            new JsonRestResponseHandler().serialize(instance, bos);

            ClientResponse clientResponse = doRequest(context, connectionString, new HashMap<String, String>(), bos.toString(CHARACTER_ENCODING), REQUEST_MODE.POST);
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
        doRequest(context, connectionString, new HashMap<String, String>(), null, REQUEST_MODE.DELETE);
    }
}
