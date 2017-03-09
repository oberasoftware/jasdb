package nl.renarj.jasdb.rest.client;

import com.google.common.collect.Maps;
import com.oberasoftware.jasdb.acl.UserSessionImpl;
import nl.renarj.jasdb.api.acl.UserSession;
import nl.renarj.jasdb.core.exceptions.ConfigurationException;
import nl.renarj.jasdb.core.locator.NodeInformation;
import nl.renarj.jasdb.remote.RemotingContext;
import nl.renarj.jasdb.remote.TokenConnector;
import nl.renarj.jasdb.remote.exceptions.RemoteException;
import nl.renarj.jasdb.rest.exceptions.RestException;
import nl.renarj.jasdb.rest.model.streaming.OauthToken;
import nl.renarj.jasdb.rest.serializers.json.JsonRestResponseHandler;

import java.util.Map;

/**
 * @author Renze de Vries
 */
public class OAuthConnector extends RemoteRestConnector implements TokenConnector {
    public OAuthConnector(NodeInformation nodeInformation) throws ConfigurationException {
        super(nodeInformation);
    }

    @Override
    public UserSession loadSession(String userName, String password) throws RemoteException {
        Map<String, String> parameters = Maps.newHashMap();
        parameters.put("client_id", userName);
        parameters.put("client_secret", password);
        ClientResponse response = doRequest(new RemotingContext(true), "token", parameters, "", REQUEST_MODE.POST);
        try {
            OauthToken token = new JsonRestResponseHandler().deserialize(OauthToken.class, response.getEntityInputStream());

            return new UserSessionImpl(token.getSessionId(), token.getOauthToken(), null, null);
        } catch(RestException e) {
            throw new RemoteException("Unable to retrieve authentication token", e);
        } finally {
            response.close();
        }
    }
}
