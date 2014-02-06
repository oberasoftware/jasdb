package nl.renarj.jasdb.rest.security;

import com.obera.service.acl.UserSessionImpl;
import nl.renarj.core.utilities.StringUtils;
import nl.renarj.jasdb.api.acl.SessionManager;
import nl.renarj.jasdb.api.acl.UserSession;
import nl.renarj.jasdb.core.SimpleKernel;
import nl.renarj.jasdb.core.crypto.CryptoEngine;
import nl.renarj.jasdb.core.crypto.CryptoFactory;
import nl.renarj.jasdb.core.exceptions.JasDBStorageException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

/**
 * @author Renze de Vries
 */
public class OAuthTokenFilter implements Filter {
    private static final Logger LOG = LoggerFactory.getLogger(OAuthTokenFilter.class);

    private static final String GRANT_INVALID = "{\"grant\":\"invalid\",\"message\":\"%s\"}";
    private static final int UNAUTHORIZED_CODE = 401;

    @Override
    public void init(FilterConfig filterConfig) throws ServletException {

    }

    @Override
    public void doFilter(ServletRequest servletRequest, ServletResponse servletResponse, FilterChain filterChain) throws IOException, ServletException {
        HttpServletRequest httpServletRequest = (HttpServletRequest) servletRequest;
        HttpServletResponse httpServletResponse = (HttpServletResponse) servletResponse;
        LOG.debug("Intercepting servlet request on path: {}", httpServletRequest.getRequestURI());
        if(!httpServletRequest.getRequestURI().equals("/token")) {
            try {
                String token = httpServletRequest.getHeader("oauth_token");
                String sessionId = httpServletRequest.getHeader("sessionid");
                LOG.debug("Token: {} for session: {}", token, sessionId);

                if(StringUtils.stringNotEmpty(token) && StringUtils.stringNotEmpty(sessionId)) {
                    SessionManager sessionManager = SimpleKernel.getKernelModule(SessionManager.class);
                    UserSession session = sessionManager.getSession(sessionId);

                    if(session != null) {
                        CryptoEngine cryptoEngine = CryptoFactory.getEngine();
                        String expectedTokenHash = cryptoEngine.hash(sessionId, token);
                        if (expectedTokenHash.equals(session.getAccessToken())) {
                            httpServletRequest.setAttribute("session", new UserSessionImpl(sessionId, token, session.getEncryptedContentKey(), session.getUser()));
                            filterChain.doFilter(servletRequest, servletResponse);
                        } else {
                            handleErrorResponse(httpServletResponse, UNAUTHORIZED_CODE, "Invalid token");
                        }
                    } else {
                        handleErrorResponse(httpServletResponse, UNAUTHORIZED_CODE, "Invalid token");
                    }
                } else {
                    handleErrorResponse(httpServletResponse, UNAUTHORIZED_CODE, "No token");
                }
            } catch(JasDBStorageException e) {
                LOG.error("Unknown error happend when processing token", e);
                handleErrorResponse(httpServletResponse, 500, "Unknown error");
            }
        } else {
            filterChain.doFilter(servletRequest, servletResponse);
        }
    }

    public void handleErrorResponse(HttpServletResponse response, int code, String message) throws IOException {
        response.setStatus(code);
        response.getWriter().print(String.format(GRANT_INVALID, message));
    }

    @Override
    public void destroy() {

    }
}
