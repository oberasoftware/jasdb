package com.oberasoftware.jasdb.rest.service.loaders;

import com.oberasoftware.jasdb.api.engine.DBInstanceFactory;
import com.oberasoftware.jasdb.api.exceptions.ConfigurationException;
import com.oberasoftware.jasdb.api.security.UserSession;
import com.oberasoftware.jasdb.api.session.DBInstance;
import com.oberasoftware.jasdb.core.context.RequestContext;
import com.oberasoftware.jasdb.rest.model.RestEntity;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import javax.servlet.http.HttpServletRequest;

/**
 * @author renarj
 */
public class DataUtil {
    public static DBInstance getInstance(DBInstanceFactory instanceFactory, String instanceId) throws ConfigurationException {
        return instanceId != null ? instanceFactory.getInstance(instanceId) : instanceFactory.getInstance();
    }

    public static ResponseEntity<RestEntity> ok(RestEntity entity) {
        return response(entity, HttpStatus.OK);
    }

    public static ResponseEntity<RestEntity> notFound(RestEntity entity) {
        return response(entity, HttpStatus.NOT_FOUND);
    }

    public static ResponseEntity<RestEntity> response(RestEntity entity, int status) {
        return response(entity, HttpStatus.valueOf(status));
    }

    public static ResponseEntity<RestEntity> response(RestEntity entity, HttpStatus status) {
        return new ResponseEntity<>(entity, status);
    }

    public static RequestContext getRequestContext(HttpServletRequest request) {
        boolean isClientRequest = RequestContextUtil.isClientRequest(request.getHeader("requestcontext"));

        RequestContext context = new RequestContext(isClientRequest, request.isSecure());
        UserSession session = (UserSession) request.getAttribute("session");
        context.setUserSession(session);

        return context;
    }
}
