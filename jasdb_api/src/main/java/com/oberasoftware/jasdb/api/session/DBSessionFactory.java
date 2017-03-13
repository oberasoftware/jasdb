package com.oberasoftware.jasdb.api.session;

import com.oberasoftware.jasdb.api.exceptions.JasDBException;
import com.oberasoftware.jasdb.api.exceptions.JasDBStorageException;
import com.oberasoftware.jasdb.api.security.Credentials;

/**
 *  The session factory is capable of creation JasDB session
 *
 *  @author Renze de Vries
 */
public interface DBSessionFactory {
    /**
     * Creates a DB Session
     * @return The DB Session
     * @throws JasDBStorageException If unable to create the session
     */
    DBSession createSession() throws JasDBException;

    /**
     * Creates a DB Session with given credentials
     * @param credentials the credentials
     * @return The DB Session
     * @throws JasDBStorageException If unable to create the session
     */
    DBSession createSession(Credentials credentials) throws JasDBException;

    /**
     * Creates a session for the given instance
     * @param instance the instance id
     * @return The session for that instance
     * @throws JasDBStorageException If unable to create the session
     */
    DBSession createSession(String instance) throws JasDBException;

    /**
     * Creates a session for the given instance and credentials
     * @param instance the instance id
     * @param credentials the credentials
     * @return The DB Session
     * @throws JasDBStorageException If unable to create the session
     */
    DBSession createSession(String instance, Credentials credentials) throws JasDBException;

    void shutdown() throws JasDBException;
}
