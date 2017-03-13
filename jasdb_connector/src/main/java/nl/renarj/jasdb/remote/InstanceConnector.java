package nl.renarj.jasdb.remote;

import com.oberasoftware.jasdb.api.model.Instance;
import nl.renarj.jasdb.remote.exceptions.RemoteException;

import java.util.List;

/**
 * @author Renze de Vries
 */
public interface InstanceConnector extends RemoteConnector {
    Instance getInstance(RemotingContext context, String instanceId) throws RemoteException;

    List<Instance> getInstances(RemotingContext context) throws RemoteException;

    Instance addInstance(RemotingContext context, String instanceId) throws RemoteException;

    void removeInstance(RemotingContext context, String instanceId) throws RemoteException;
}
