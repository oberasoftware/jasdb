package nl.renarj.jasdb.api.acl;

import nl.renarj.jasdb.api.context.Credentials;
import nl.renarj.jasdb.api.metadata.GrantObject;
import nl.renarj.jasdb.api.metadata.User;
import nl.renarj.jasdb.core.exceptions.JasDBStorageException;

import java.util.List;

/**
 * @author Renze de Vries
 */
public interface UserManager {

    List<String> getUsers(UserSession currentSession) throws JasDBStorageException;

    User authenticate(Credentials credentials) throws JasDBStorageException;

    void authorize(UserSession userSession, String object, AccessMode mode) throws JasDBStorageException;

    User addUser(UserSession currentSession, String userName, String allowedHost, String password) throws JasDBStorageException;

    GrantObject getGrantObject(UserSession session, String object) throws JasDBStorageException;

    List<GrantObject> getGrantObjects(UserSession session) throws JasDBStorageException;

    void grantUser(UserSession currentSession, String object, String userName, AccessMode mode) throws JasDBStorageException;

    void revoke(UserSession currentSession, String object, String userName) throws JasDBStorageException;

    void deleteUser(UserSession currentSession, String userName) throws JasDBStorageException;
}
