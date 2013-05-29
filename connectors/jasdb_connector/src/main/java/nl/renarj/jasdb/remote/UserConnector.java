package nl.renarj.jasdb.remote;

import nl.renarj.jasdb.api.acl.AccessMode;
import nl.renarj.jasdb.remote.exceptions.RemoteException;

import java.util.List;

/**
 * @author Renze de Vries
 */
public interface UserConnector extends RemoteConnector {
    void addUser(RemotingContext context, String username, String host, String password) throws RemoteException;

    void deleteUser(RemotingContext context, String username) throws RemoteException;

    List<String> getUsers(RemotingContext context) throws RemoteException;

    void grant(RemotingContext context, String object, String user, AccessMode mode) throws RemoteException;

    void revoke(RemotingContext context, String object, String user) throws RemoteException;
}
