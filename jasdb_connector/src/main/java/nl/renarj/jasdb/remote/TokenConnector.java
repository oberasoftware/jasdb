package nl.renarj.jasdb.remote;

import nl.renarj.jasdb.api.acl.UserSession;
import nl.renarj.jasdb.remote.exceptions.RemoteException;

/**
 * @author Renze de Vries
 */
public interface TokenConnector extends RemoteConnector {
    UserSession loadSession(String userName, String password) throws RemoteException;
}
