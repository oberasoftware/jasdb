package nl.renarj.jasdb.rest.security;

import com.obera.service.acl.BasicCredentials;
import nl.renarj.jasdb.api.acl.SessionManager;
import nl.renarj.jasdb.api.acl.UserSession;
import nl.renarj.jasdb.core.SimpleKernel;
import nl.renarj.jasdb.core.exceptions.JasDBSecurityException;
import nl.renarj.jasdb.core.exceptions.JasDBStorageException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

/**
 * @author Renze de Vries
 */
public class OAuthTokenEndpoint extends HttpServlet {
    private static final Logger LOG = LoggerFactory.getLogger(OAuthTokenEndpoint.class);

    private static final String GRANT_INVALID = "{\"grant\":\"invalid\",\"message\":\"%s\"}";
    private static final String GRANT_VALID = "{\"grant\":\"valid\",\"access_token\":\"%s\",\"sessionid\":\"%s\",\"token_type\":\"%s\",\"expires_in\":%d}";
    private static final int UNAUTHORIZED_CODE = 401;



    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        handleErrorResponse(resp, "GET not supported");
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        resp.setContentType("application/json");
        if(req.isSecure()) {
            try {
                SessionManager sessionManager = SimpleKernel.getKernelModule(SessionManager.class);
                String clientId = req.getParameter("client_id");
                String clientSecret = req.getParameter("client_secret");
                LOG.debug("Client: {} host: {}", clientId, req.getRemoteHost());

                UserSession session = sessionManager.startSession(new BasicCredentials(clientId, req.getRemoteHost(), clientSecret));
                LOG.debug("Loaded session: {}", session);
                resp.getWriter().print(String.format(GRANT_VALID, session.getAccessToken(), session.getSessionId(), "jasdb", 3600));
            } catch(JasDBSecurityException e) {
                handleErrorResponse(resp, "Invalid credentials");
            } catch(JasDBStorageException e) {
                handleErrorResponse(resp, "Unknown error");
            }
        } else {
            handleErrorResponse(resp, "Insecure connection");
        }
    }

    public void handleErrorResponse(HttpServletResponse response, String message) throws IOException {
        response.setStatus(UNAUTHORIZED_CODE);
        response.getWriter().print(String.format(GRANT_INVALID, message));
    }
}
