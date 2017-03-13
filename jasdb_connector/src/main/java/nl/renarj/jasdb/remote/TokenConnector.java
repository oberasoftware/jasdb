package nl.renarj.jasdb.remote;

import com.oberasoftware.jasdb.api.security.UserSession;
import nl.renarj.jasdb.remote.exceptions.RemoteException;

/**
 * @author Renze de Vries
 */
public interface TokenConnector extends RemoteConnector {
    UserSession loadSession(String userName, String password) throws RemoteException;
}
