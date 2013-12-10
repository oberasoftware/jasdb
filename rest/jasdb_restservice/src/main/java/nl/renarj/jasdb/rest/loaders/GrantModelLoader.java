package nl.renarj.jasdb.rest.loaders;

import nl.renarj.core.utilities.StringUtils;
import nl.renarj.jasdb.api.acl.UserManager;
import nl.renarj.jasdb.api.context.RequestContext;
import nl.renarj.jasdb.api.metadata.GrantObject;
import nl.renarj.jasdb.core.SimpleKernel;
import nl.renarj.jasdb.core.exceptions.JasDBStorageException;
import nl.renarj.jasdb.rest.exceptions.RestException;
import nl.renarj.jasdb.rest.input.InputElement;
import nl.renarj.jasdb.rest.input.OrderParam;
import nl.renarj.jasdb.rest.input.conditions.FieldCondition;
import nl.renarj.jasdb.rest.mappers.GrantModelMapper;
import nl.renarj.jasdb.rest.model.RestEntity;
import nl.renarj.jasdb.rest.model.RestGrant;
import nl.renarj.jasdb.rest.model.RestGrantObject;
import nl.renarj.jasdb.rest.model.RestGrantObjectCollection;
import nl.renarj.jasdb.rest.serializers.RestResponseHandler;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Renze de Vries
 */
public class GrantModelLoader extends AbstractModelLoader {
    @Override
    public String[] getModelNames() {
        return new String[] { "Grants" };
    }

    @Override
    public RestEntity loadModel(InputElement input, String begin, String top, List<OrderParam> orderParamList, RequestContext requestContext) throws RestException {
        if(input.getCondition() != null) {
            return loadSpecificGrantObject(requestContext, ((FieldCondition) input.getCondition()).getValue());
        } else {
            return loadAllGrantObjects(requestContext);
        }
    }

    private RestEntity loadSpecificGrantObject(RequestContext context, String object) throws RestException {
        try {
            UserManager userManager = SimpleKernel.getKernelModule(UserManager.class);
            GrantObject grantObject = userManager.getGrantObject(context.getUserSession(), object);

            return GrantModelMapper.map(grantObject);
        } catch(JasDBStorageException e) {
            throw new RestException("Unable to load grant objects", e);
        }
    }

    private RestEntity loadAllGrantObjects(RequestContext context) throws RestException {
        try {
            UserManager userManager = SimpleKernel.getKernelModule(UserManager.class);
            List<GrantObject> grantObjects = userManager.getGrantObjects(context.getUserSession());
            List<RestGrantObject> restGrantObjects = new ArrayList<>();
            for(GrantObject grantObject : grantObjects) {
                restGrantObjects.add(GrantModelMapper.map(grantObject));
            }

            return new RestGrantObjectCollection(restGrantObjects);
        } catch(JasDBStorageException e) {
            throw new RestException("Unable to load grant objects", e);
        }

    }

    @Override
    public RestEntity writeEntry(InputElement input, RestResponseHandler serializer, String rawData, RequestContext requestContext) throws RestException {
        if(requestContext.isSecure()) {
            RestGrant grant = serializer.deserialize(RestGrant.class, rawData);

            if(StringUtils.stringNotEmpty(grant.getObjectName()) && StringUtils.stringNotEmpty(grant.getUsername())) {
                try {
                    UserManager userManager = SimpleKernel.getKernelModule(UserManager.class);
                    userManager.grantUser(requestContext.getUserSession(), grant.getObjectName(), grant.getUsername(), grant.getMode());

                    return loadSpecificGrantObject(requestContext, grant.getObjectName());
                } catch(JasDBStorageException e) {
                    throw new RestException("Unable to grant", e);
                }
            } else {
                throw new RestException("Incomplete grant details");
            }
        } else {
            throw new RestException("Unable to create grant, unsecure connection");
        }
    }

    @Override
    public RestEntity removeEntry(InputElement input, RestResponseHandler serializer, String rawData, RequestContext requestContext) throws RestException {
        RestGrant grant = serializer.deserialize(RestGrant.class, rawData);
        if(StringUtils.stringNotEmpty(grant.getObjectName()) && StringUtils.stringNotEmpty(grant.getUsername())) {
            try {
                UserManager userManager = SimpleKernel.getKernelModule(UserManager.class);
                userManager.revoke(requestContext.getUserSession(), grant.getObjectName(), grant.getUsername());

                return null;
            } catch(JasDBStorageException e) {
                throw new RestException("Unable to revoke grant", e);
            }
        } else {
            throw new RestException("Cannot remove without user and object specified");
        }
    }
}
