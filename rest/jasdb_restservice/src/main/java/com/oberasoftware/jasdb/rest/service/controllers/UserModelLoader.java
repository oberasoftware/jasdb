package com.oberasoftware.jasdb.rest.service.controllers;

import com.oberasoftware.jasdb.api.exceptions.JasDBStorageException;
import com.oberasoftware.jasdb.api.exceptions.RestException;
import com.oberasoftware.jasdb.api.security.UserManager;
import com.oberasoftware.jasdb.core.context.RequestContext;
import com.oberasoftware.jasdb.core.utils.StringUtils;
import com.oberasoftware.jasdb.rest.model.RestEntity;
import com.oberasoftware.jasdb.rest.model.RestUser;
import com.oberasoftware.jasdb.rest.model.RestUserList;
import jakarta.servlet.http.HttpServletRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

import static org.springframework.web.bind.annotation.RequestMethod.*;

/**
 * @author Renze de Vries
 */
@RestController
public class UserModelLoader {
    private static final Logger LOG = LoggerFactory.getLogger(UserModelLoader.class);

    private UserManager userManager;

    @Autowired(required = false)
    public UserModelLoader(UserManager userManager) {
        this.userManager = userManager;
    }

    public UserModelLoader() {
    }

    @RequestMapping(value = "/Users", produces = "application/json", method = GET)
    public RestEntity loadModel(RequestContext requestContext) throws RestException {
        return loadUserList(requestContext);
    }

    private RestEntity loadUserList(RequestContext context) throws RestException {
        try {
            List<String> userList = userManager.getUsers(context.getUserSession());

            return new RestUserList(userList);
        } catch(JasDBStorageException e) {
            throw new RestException("Unable to load user list", e);
        }
    }

    @RequestMapping(value = "/Users", produces = "application/json", consumes = "application/json", method = POST)
    public RestEntity writeEntry(@RequestBody  RestUser user, HttpServletRequest request) throws RestException {
        RequestContext requestContext = ControllerUtil.getRequestContext(request);
        if(requestContext.isSecure()) {
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

    @RequestMapping(value = "/Users({userId})", produces = "application/json", consumes = "application/json", method = DELETE)
    public RestEntity removeEntry(@PathVariable String userId, HttpServletRequest request) throws RestException {
        if (StringUtils.stringNotEmpty(userId)) {
            try {
                RequestContext requestContext = ControllerUtil.getRequestContext(request);
                userManager.deleteUser(requestContext.getUserSession(), userId);

                return null;
            } catch (JasDBStorageException e) {
                throw new RestException("Unable to remove user", e);
            }
        } else {
            throw new RestException("Unable to delete user, no id specified");
        }
    }
}
