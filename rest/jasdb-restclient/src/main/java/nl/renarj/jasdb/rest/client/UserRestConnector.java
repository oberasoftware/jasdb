package nl.renarj.jasdb.rest.client;

import nl.renarj.jasdb.api.acl.AccessMode;
import nl.renarj.jasdb.core.exceptions.ConfigurationException;
import nl.renarj.jasdb.core.locator.NodeInformation;
import nl.renarj.jasdb.remote.RemotingContext;
import nl.renarj.jasdb.remote.UserConnector;
import nl.renarj.jasdb.remote.exceptions.RemoteException;
import nl.renarj.jasdb.rest.exceptions.RestException;
import nl.renarj.jasdb.rest.model.RestGrant;
import nl.renarj.jasdb.rest.model.RestUser;
import nl.renarj.jasdb.rest.model.RestUserList;
import nl.renarj.jasdb.rest.serializers.json.JsonRestResponseHandler;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * @author Renze de Vries
 */
public class UserRestConnector extends RemoteRestConnector implements UserConnector {
    public UserRestConnector(NodeInformation nodeInformation) throws ConfigurationException {
        super(nodeInformation);
    }

    @Override
    public void addUser(RemotingContext context, String username, String host, String password) throws RemoteException {
        try {
            byte[] serializedUser = toBytes(new RestUser(username, host, password));
            doInternalRequest(context, "Users", new HashMap<String, String>(), serializedUser, REQUEST_MODE.POST);
        } catch(RestException e) {
            throw new RemoteException("Unable to add user", e);
        }
    }

    @Override
    public void deleteUser(RemotingContext context, String username) throws RemoteException {
        StringBuilder builder = new StringBuilder();
        builder.append("Users(").append(username).append(")");
        doRequest(context, builder.toString(), new HashMap<String, String>(), null, REQUEST_MODE.DELETE);
    }

    @Override
    public List<String> getUsers(RemotingContext context) throws RemoteException {
        ClientResponse clientResponse = doRequest(context, "Users");
        try {
            RestUserList userList = new JsonRestResponseHandler().deserialize(RestUserList.class, clientResponse.getEntityInputStream());
            List<String> userNames = new ArrayList<>();
            for(RestUser restUser : userList.getUsers()) {
                userNames.add(restUser.getUsername());
            }
            return userNames;
        } catch(RestException e) {
            throw new RemoteException("Unable to retrieve remote user list", e);
        }
    }

    @Override
    public void grant(RemotingContext context, String object, String user, AccessMode mode) throws RemoteException {
        try {
            byte[] serializedGrant = toBytes(new RestGrant(user, object, mode));
            doInternalRequest(context, "Grants", new HashMap<String, String>(), serializedGrant, REQUEST_MODE.POST);
        } catch(RestException e) {
            throw new RemoteException("Unable to grant", e);
        }
    }

    @Override
    public void revoke(RemotingContext context, String object, String user) throws RemoteException {
        try {
            byte[] serializedGrant = toBytes(new RestGrant(user, object, AccessMode.NONE));
            doInternalRequest(context, "Grants", new HashMap<String, String>(), serializedGrant, REQUEST_MODE.DELETE);
        } catch(RestException e) {
            throw new RemoteException("Unable to grant", e);
        }
    }
}
