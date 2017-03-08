package nl.renarj.jasdb.rest.loaders;

import nl.renarj.core.utilities.StringUtils;
import nl.renarj.jasdb.api.acl.UserManager;
import nl.renarj.jasdb.api.context.RequestContext;
import nl.renarj.jasdb.core.exceptions.JasDBStorageException;
import nl.renarj.jasdb.rest.exceptions.RestException;
import nl.renarj.jasdb.rest.input.InputElement;
import nl.renarj.jasdb.rest.input.OrderParam;
import nl.renarj.jasdb.rest.input.conditions.FieldCondition;
import nl.renarj.jasdb.rest.input.conditions.InputCondition;
import nl.renarj.jasdb.rest.model.RestEntity;
import nl.renarj.jasdb.rest.model.RestUser;
import nl.renarj.jasdb.rest.model.RestUserList;
import nl.renarj.jasdb.rest.serializers.RestResponseHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * @author Renze de Vries
 */
@Component
public class UserModelLoader extends AbstractModelLoader {
    private static final Logger LOG = LoggerFactory.getLogger(UserModelLoader.class);

    @Autowired(required = false)
    private UserManager userManager;

    @Override
    public String[] getModelNames() {
        return new String[] { "Users" };
    }

    @Override
    public RestEntity loadModel(InputElement input, String begin, String top, List<OrderParam> orderParamList, RequestContext requestContext) throws RestException {
        InputCondition condition = input.getCondition();
        if (condition == null) {
            return loadUserList(requestContext);
        } else {
            throw new RestException("Querying of users not supported");
        }
    }

    private RestEntity loadUserList(RequestContext context) throws RestException {
        try {
            List<String> userList = userManager.getUsers(context.getUserSession());

            return new RestUserList(userList);
        } catch(JasDBStorageException e) {
            throw new RestException("Unable to load user list", e);
        }
    }

    @Override
    public RestEntity writeEntry(InputElement input, RestResponseHandler serializer, String rawData, RequestContext requestContext) throws RestException {
        if(requestContext.isSecure()) {
            RestUser user = serializer.deserialize(RestUser.class, rawData);
            if(StringUtils.stringNotEmpty(user.getUsername()) && StringUtils.stringNotEmpty(user.getAllowedHost()) && StringUtils.stringNotEmpty(user.getPassword())) {
                try {
                    userManager.addUser(requestContext.getUserSession(), user.getUsername(), user.getAllowedHost(), user.getPassword());

                    return new RestUser(user.getUsername(), user.getAllowedHost(), null);
                } catch(JasDBStorageException e) {
                    LOG.error("", e);
                    throw new RestException("Unable to create user", e);
                }
            } else {
                throw new RestException("Incomplete user details");
            }
        } else {
            throw new RestException("Unable to create user, unsecure connection");
        }
    }

    @Override
    public RestEntity removeEntry(InputElement input, RestResponseHandler serializer, String rawData, RequestContext requestContext) throws RestException {
        if(input.getCondition() != null) {
            try {
                userManager.deleteUser(requestContext.getUserSession(), ((FieldCondition) input.getCondition()).getValue());

                return null;
            } catch(JasDBStorageException e) {
                throw new RestException("Unable to remove user", e);
            }
        } else {
            throw new RestException("Unable to remove user, not specified");
        }
    }
}
