package com.oberasoftware.jasdb.rest.service.controllers;

import com.oberasoftware.jasdb.api.exceptions.JasDBStorageException;
import com.oberasoftware.jasdb.api.exceptions.RestException;
import com.oberasoftware.jasdb.api.model.GrantObject;
import com.oberasoftware.jasdb.api.security.UserManager;
import com.oberasoftware.jasdb.core.context.RequestContext;
import com.oberasoftware.jasdb.core.utils.StringUtils;
import com.oberasoftware.jasdb.rest.model.RestEntity;
import com.oberasoftware.jasdb.rest.model.RestGrant;
import com.oberasoftware.jasdb.rest.model.RestGrantObject;
import com.oberasoftware.jasdb.rest.model.RestGrantObjectCollection;
import com.oberasoftware.jasdb.rest.model.mappers.GrantModelMapper;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.ArrayList;
import java.util.List;

import static com.oberasoftware.jasdb.rest.service.controllers.ControllerUtil.getRequestContext;
import static org.springframework.web.bind.annotation.RequestMethod.DELETE;
import static org.springframework.web.bind.annotation.RequestMethod.POST;

/**
 * @author Renze de Vries
 */
@RestController
public class GrantController {

    private UserManager userManager;

    @Autowired(required = false)
    public GrantController(UserManager userManager) {
        this.userManager = userManager;
    }

    public GrantController() {
    }

    @RequestMapping(value = "/Grants", produces = "application/json", method = RequestMethod.GET)
    public RestEntity getAllGrants(HttpServletRequest request) throws RestException {
        return loadAllGrantObjects(getRequestContext(request));
    }

    @RequestMapping(value = "/Grants({grantId})", produces = "application/json", method = RequestMethod.GET)
    public RestEntity getGrant(@PathVariable String grantId, HttpServletRequest request) throws RestException {
        return loadSpecificGrantObject(getRequestContext(request), grantId);
    }


    private RestEntity loadSpecificGrantObject(RequestContext context, String object) throws RestException {
        try {
            GrantObject grantObject = userManager.getGrantObject(context.getUserSession(), object);

            return GrantModelMapper.map(grantObject);
        } catch(JasDBStorageException e) {
            throw new RestException("Unable to load grant objects", e);
        }
    }

    private RestEntity loadAllGrantObjects(RequestContext context) throws RestException {
        try {
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

    @RequestMapping(value = "/Grants", consumes = "application/json", produces = "application/json", method = POST)
    public RestEntity writeEntry(@RequestBody RestGrant grant, HttpServletRequest request) throws RestException {
        RequestContext requestContext = getRequestContext(request);
        if(requestContext.isSecure()) {
            if(StringUtils.stringNotEmpty(grant.getObjectName()) && StringUtils.stringNotEmpty(grant.getUsername())) {
                try {
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

    @RequestMapping(value = "/Grants({grantId})", produces = "application/json", method = DELETE)
    public RestEntity removeEntry(@RequestBody RestGrant grant, HttpServletRequest request) throws RestException {
        if(StringUtils.stringNotEmpty(grant.getObjectName()) && StringUtils.stringNotEmpty(grant.getUsername())) {
            try {
                RequestContext requestContext = getRequestContext(request);
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
