package nl.renarj.jasdb.api;

import com.oberasoftware.jasdb.api.session.UserAdministration;
import com.oberasoftware.jasdb.api.security.AccessMode;
import com.oberasoftware.jasdb.api.exceptions.JasDBStorageException;
import com.oberasoftware.jasdb.api.model.NodeInformation;
import nl.renarj.jasdb.remote.RemoteConnectorFactory;
import nl.renarj.jasdb.remote.RemotingContext;
import nl.renarj.jasdb.remote.UserConnector;

import java.util.List;

/**
 * @author Renze de Vries
 */
public class RemoteUserAdministration implements UserAdministration {
    private RemotingContext remotingContext;
    private NodeInformation nodeInformation;

    public RemoteUserAdministration(NodeInformation nodeInformation, RemotingContext remotingContext) {
        this.remotingContext = remotingContext;
        this.nodeInformation = nodeInformation;
    }

    @Override
    public void addUser(String username, String allowedHost, String password) throws JasDBStorageException {
        UserConnector userConnector = RemoteConnectorFactory.createConnector(nodeInformation, UserConnector.class);
        userConnector.addUser(remotingContext, username, allowedHost, password);
    }

    @Override
    public void deleteUser(String username) throws JasDBStorageException {
        UserConnector userConnector = RemoteConnectorFactory.createConnector(nodeInformation, UserConnector.class);
        userConnector.deleteUser(remotingContext, username);
    }

    @Override
    public List<String> getUsers() throws JasDBStorageException {
        UserConnector userConnector = RemoteConnectorFactory.createConnector(nodeInformation, UserConnector.class);
        return userConnector.getUsers(remotingContext);
    }

    @Override
    public void grant(String username, String object, AccessMode mode) throws JasDBStorageException {
        UserConnector userConnector = RemoteConnectorFactory.createConnector(nodeInformation, UserConnector.class);
        userConnector.grant(remotingContext, object, username, mode);
    }

    @Override
    public void revoke(String username, String object) throws JasDBStorageException {
        UserConnector userConnector = RemoteConnectorFactory.createConnector(nodeInformation, UserConnector.class);
        userConnector.revoke(remotingContext, object, username);
    }
}
