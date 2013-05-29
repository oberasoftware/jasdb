package nl.renarj.jasdb.api.acl;

import nl.renarj.jasdb.api.context.Credentials;
import nl.renarj.jasdb.core.exceptions.JasDBStorageException;

import java.util.List;

/**
 * @author Renze de Vries
 */
public interface SessionManager {
    UserSession startSession(Credentials credentials) throws JasDBStorageException;

    boolean sessionValid(String sessionId) throws JasDBStorageException;

    List<UserSession> getActiveSessions() throws JasDBStorageException;

    void endSession(String sessionId) throws JasDBStorageException;

    UserSession getSession(String sessionId) throws JasDBStorageException;
}
