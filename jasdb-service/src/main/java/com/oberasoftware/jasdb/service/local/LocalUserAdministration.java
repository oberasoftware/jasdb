package com.oberasoftware.jasdb.service.local;

import com.oberasoftware.jasdb.api.session.UserAdministration;
import com.oberasoftware.jasdb.api.security.AccessMode;
import com.oberasoftware.jasdb.api.security.SessionManager;
import com.oberasoftware.jasdb.api.security.UserManager;
import com.oberasoftware.jasdb.api.security.UserSession;
import com.oberasoftware.jasdb.api.exceptions.JasDBSecurityException;
import com.oberasoftware.jasdb.api.exceptions.JasDBStorageException;

import java.util.List;

/**
 * @author Renze de Vries
 */
public class LocalUserAdministration implements UserAdministration {
    private UserSession session;
    private UserManager userManager;
    private SessionManager sessionManager;

    public LocalUserAdministration(UserSession session) throws JasDBStorageException {
        this.session = session;
        userManager = ApplicationContextProvider.getApplicationContext().getBean(UserManager.class);
        sessionManager = ApplicationContextProvider.getApplicationContext().getBean(SessionManager.class);
    }

    @Override
    public void addUser(String username, String allowedHost, String password) throws JasDBStorageException {
        validateSession();
        userManager.addUser(session, username, allowedHost, password);
    }

    @Override
    public void deleteUser(String username) throws JasDBStorageException {
        validateSession();
        userManager.deleteUser(session, username);
    }

    @Override
    public List<String> getUsers() throws JasDBStorageException {
        return userManager.getUsers(session);
    }

    @Override
    public void grant(String username, String object, AccessMode mode) throws JasDBStorageException {
        validateSession();
        userManager.grantUser(session, object, username, mode);
    }

    @Override
    public void revoke(String username, String object) throws JasDBStorageException {
        validateSession();
        userManager.revoke(session, object, username);
    }

    private void validateSession() throws JasDBStorageException {
        if(session == null || !sessionManager.sessionValid(session.getSessionId())) {
            throw new JasDBSecurityException("Unable to change security principals, not logged in or session expired");
        }
    }
}
