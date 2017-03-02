package nl.renarj.jasdb.remote;

import nl.renarj.jasdb.api.metadata.Instance;
import nl.renarj.jasdb.remote.exceptions.RemoteException;

import java.util.List;

/**
 * @author Renze de Vries
 */
public interface InstanceConnector extends RemoteConnector {
    Instance getInstance(RemotingContext context, String instanceId) throws RemoteException;

    List<Instance> getInstances(RemotingContext context) throws RemoteException;

    Instance addInstance(RemotingContext context, String instanceId, String instancePath) throws RemoteException;

    void removeInstance(RemotingContext context, String instanceId) throws RemoteException;
}
