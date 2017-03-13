package com.oberasoftware.jasdb.rest.client;

import com.oberasoftware.jasdb.api.security.AccessMode;
import com.oberasoftware.jasdb.api.exceptions.ConfigurationException;
import com.oberasoftware.jasdb.api.model.NodeInformation;
import nl.renarj.jasdb.remote.RemotingContext;
import nl.renarj.jasdb.remote.UserConnector;
import nl.renarj.jasdb.remote.exceptions.RemoteException;
import com.oberasoftware.jasdb.api.exceptions.RestException;
import com.oberasoftware.jasdb.rest.model.RestGrant;
import com.oberasoftware.jasdb.rest.model.RestUser;
import com.oberasoftware.jasdb.rest.model.RestUserList;
import com.oberasoftware.jasdb.rest.model.serializers.json.JsonRestResponseHandler;

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
            doInternalRequest(context, "Users", new HashMap<>(), serializedUser, REQUEST_MODE.POST);
        } catch(RestException e) {
            throw new RemoteException("Unable to add user", e);
        }
    }

    @Override
    public void deleteUser(RemotingContext context, String username) throws RemoteException {
        StringBuilder builder = new StringBuilder();
        builder.append("Users(").append(username).append(")");
        doRequest(context, builder.toString(), new HashMap<>(), null, REQUEST_MODE.DELETE);
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
            doInternalRequest(context, "Grants", new HashMap<>(), serializedGrant, REQUEST_MODE.POST);
        } catch(RestException e) {
            throw new RemoteException("Unable to grant", e);
        }
    }

    @Override
    public void revoke(RemotingContext context, String object, String user) throws RemoteException {
        try {
            byte[] serializedGrant = toBytes(new RestGrant(user, object, AccessMode.NONE));
            doInternalRequest(context, "Grants", new HashMap<>(), serializedGrant, REQUEST_MODE.DELETE);
        } catch(RestException e) {
            throw new RemoteException("Unable to grant", e);
        }
    }
}
