package com.oberasoftware.jasdb.rest.service.security;

import com.oberasoftware.jasdb.core.security.UserSessionImpl;
import com.oberasoftware.jasdb.core.utils.StringUtils;
import com.oberasoftware.jasdb.api.security.SessionManager;
import com.oberasoftware.jasdb.api.security.UserSession;
import com.oberasoftware.jasdb.api.security.CryptoEngine;
import com.oberasoftware.jasdb.core.crypto.CryptoFactory;
import com.oberasoftware.jasdb.api.exceptions.JasDBStorageException;
import jakarta.servlet.*;
import jakarta.servlet.http.HttpServletResponse;
import jakarta.servlet.http.HttpServletRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;

/**
 * @author Renze de Vries
 */
@Component
public class OAuthTokenFilter implements Filter {
    private static final Logger LOG = LoggerFactory.getLogger(OAuthTokenFilter.class);

    private static final String GRANT_INVALID = "{\"grant\":\"invalid\",\"message\":\"%s\"}";
    private static final int UNAUTHORIZED_CODE = 401;

    @Autowired(required = false)
    private SessionManager sessionManager;

    private boolean enabled = false;

    @Override
    public void init(FilterConfig filterConfig) throws ServletException {

    }

    @Override
    public void doFilter(ServletRequest servletRequest, ServletResponse servletResponse, FilterChain filterChain) throws IOException, ServletException {
        HttpServletRequest httpServletRequest = (HttpServletRequest) servletRequest;
        HttpServletResponse httpServletResponse = (HttpServletResponse) servletResponse;
        LOG.debug("Intercepting servlet request on path: {}", httpServletRequest.getRequestURI());
        if(!httpServletRequest.getRequestURI().equals("/token")) {
            if(enabled) {
                checkToken(httpServletRequest, httpServletResponse, filterChain);
            } else {
                filterChain.doFilter(servletRequest, servletResponse);
            }
        } else {
            filterChain.doFilter(servletRequest, servletResponse);
        }
    }

    private void checkToken(HttpServletRequest httpServletRequest, HttpServletResponse httpServletResponse, FilterChain filterChain) throws IOException, ServletException {
        try {
            String token = httpServletRequest.getHeader("oauth_token");
            String sessionId = httpServletRequest.getHeader("sessionid");
            LOG.debug("Token: {} for session: {}", token, sessionId);

            if(StringUtils.stringNotEmpty(token) && StringUtils.stringNotEmpty(sessionId)) {
                UserSession session = sessionManager.getSession(sessionId);

                if(session != null) {
                    CryptoEngine cryptoEngine = CryptoFactory.getEngine();
                    String expectedTokenHash = cryptoEngine.hash(sessionId, token);
                    if (expectedTokenHash.equals(session.getAccessToken())) {
                        httpServletRequest.setAttribute("session", new UserSessionImpl(sessionId, token, session.getEncryptedContentKey(), session.getUser()));
                        filterChain.doFilter(httpServletRequest, httpServletResponse);
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
            LOG.error("Unknown error happened when processing token", e);
            handleErrorResponse(httpServletResponse, 500, "Unknown error");
        }
    }

    private void handleErrorResponse(HttpServletResponse response, int code, String message) throws IOException {
        response.setStatus(code);
        response.getWriter().print(String.format(GRANT_INVALID, message));
    }

    @Override
    public void destroy() {

    }
}
