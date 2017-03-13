package com.oberasoftware.jasdb.api.security;

import com.oberasoftware.jasdb.api.exceptions.JasDBStorageException;

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
