package nl.renarj.jasdb.api;

import nl.renarj.jasdb.api.context.Credentials;
import nl.renarj.jasdb.core.exceptions.JasDBException;
import nl.renarj.jasdb.core.exceptions.JasDBStorageException;

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
