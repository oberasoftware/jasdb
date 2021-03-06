package nl.renarj.jasdb.remote;

import com.oberasoftware.jasdb.api.model.NodeInformation;
import nl.renarj.jasdb.remote.exceptions.RemoteException;

/**
 * @author Renze de Vries
 */
public interface ConnectorLoader {
    String getConnectorType();
    
    <T extends RemoteConnector> T createConnector(NodeInformation nodeInformation, Class<T> operationType) throws RemoteException;

    int priority();

    boolean isReusable();
    
    String getCachingKey(NodeInformation nodeInformation, Class<? extends RemoteConnector> operationType);
    
    void shutdown();
}
