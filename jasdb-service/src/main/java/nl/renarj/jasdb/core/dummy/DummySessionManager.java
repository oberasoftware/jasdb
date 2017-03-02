package nl.renarj.jasdb.core.dummy;

import nl.renarj.jasdb.api.acl.SessionManager;
import nl.renarj.jasdb.api.acl.UserSession;
import nl.renarj.jasdb.api.context.Credentials;
import nl.renarj.jasdb.core.exceptions.JasDBStorageException;

import java.util.List;

/**
 * @author Renze de Vries
 */
public class DummySessionManager implements SessionManager {
    @Override
    public UserSession startSession(Credentials credentials) throws JasDBStorageException {
        DummyUserManager.throwNotSupported();
        return null;
    }

    @Override
    public boolean sessionValid(String sessionId) throws JasDBStorageException {
        DummyUserManager.throwNotSupported();
        return false;
    }

    @Override
    public List<UserSession> getActiveSessions() throws JasDBStorageException {
        DummyUserManager.throwNotSupported();
        return null;
    }

    @Override
    public void endSession(String sessionId) throws JasDBStorageException {
        DummyUserManager.throwNotSupported();
    }

    @Override
    public UserSession getSession(String sessionId) throws JasDBStorageException {
        DummyUserManager.throwNotSupported();
        return null;
    }
}
