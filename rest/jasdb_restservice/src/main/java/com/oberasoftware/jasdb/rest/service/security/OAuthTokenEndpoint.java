package com.oberasoftware.jasdb.rest.service.security;

import com.oberasoftware.jasdb.acl.BasicCredentials;
import com.oberasoftware.jasdb.api.security.SessionManager;
import com.oberasoftware.jasdb.api.security.UserSession;
import com.oberasoftware.jasdb.api.exceptions.JasDBSecurityException;
import com.oberasoftware.jasdb.api.exceptions.JasDBStorageException;
import jakarta.servlet.http.HttpServletRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;


/**
 * @author Renze de Vries
 */
@RestController
public class OAuthTokenEndpoint {
    private static final Logger LOG = LoggerFactory.getLogger(OAuthTokenEndpoint.class);

    private static final String GRANT_INVALID = "{\"grant\":\"invalid\",\"message\":\"%s\"}";
    private static final String GRANT_VALID = "{\"grant\":\"valid\",\"access_token\":\"%s\",\"sessionid\":\"%s\",\"token_type\":\"%s\",\"expires_in\":%d}";

    @Autowired(required = false)
    private SessionManager sessionManager;

    @RequestMapping(method = RequestMethod.POST, value = "/token", produces = "application/json", consumes = "application/json")
    public @ResponseBody
    ResponseEntity<String> getToken(HttpServletRequest request) {
        if(request.isSecure()) {
            try {
                String clientId = request.getParameter("client_id");
                String clientSecret = request.getParameter("client_secret");
                LOG.debug("Client: {} host: {}", clientId, request.getRemoteHost());

                UserSession session = sessionManager.startSession(new BasicCredentials(clientId, request.getRemoteHost(), clientSecret));
                LOG.debug("Loaded session: {}", session);
                String responseMessage = String.format(GRANT_VALID, session.getAccessToken(), session.getSessionId(), "jasdb", 3600);
                return new ResponseEntity<>(responseMessage, HttpStatus.OK);
            } catch(JasDBSecurityException e) {
                return getErrorResponse("Invalid credentials");
            } catch(JasDBStorageException e) {
                return getErrorResponse("Unknown error");
            }
        } else {
            return getErrorResponse("Insecure connection");
        }
    }

    private ResponseEntity<String> getErrorResponse(String message) {
        return new ResponseEntity<>(String.format(GRANT_INVALID, message), HttpStatus.UNAUTHORIZED);
    }
}
