package nl.renarj.jasdb.core.dummy;

import nl.renarj.jasdb.api.acl.AccessMode;
import nl.renarj.jasdb.api.acl.UserManager;
import nl.renarj.jasdb.api.acl.UserSession;
import nl.renarj.jasdb.api.context.Credentials;
import nl.renarj.jasdb.api.metadata.GrantObject;
import nl.renarj.jasdb.api.metadata.User;
import nl.renarj.jasdb.core.exceptions.JasDBSecurityException;
import nl.renarj.jasdb.core.exceptions.JasDBStorageException;

import java.util.List;

/**
 * @author Renze de Vries
 */
public class DummyUserManager implements UserManager {
    @Override
    public List<String> getUsers(UserSession currentSession) throws JasDBStorageException {
        throwNotSupported();
        return null;
    }

    @Override
    public User authenticate(Credentials credentials) throws JasDBStorageException {
        throwNotSupported();
        return null;
    }

    @Override
    public void authorize(UserSession userSession, String object, AccessMode mode) throws JasDBStorageException {
        throwNotSupported();
    }

    @Override
    public User addUser(UserSession currentSession, String userName, String allowedHost, String password) throws JasDBStorageException {
        throwNotSupported();
        return null;
    }

    @Override
    public GrantObject getGrantObject(UserSession session, String object) throws JasDBStorageException {
        throwNotSupported();
        return null;
    }

    @Override
    public List<GrantObject> getGrantObjects(UserSession session) throws JasDBStorageException {
        throwNotSupported();
        return null;
    }

    @Override
    public void grantUser(UserSession currentSession, String object, String userName, AccessMode mode) throws JasDBStorageException {
        throwNotSupported();
    }

    @Override
    public void revoke(UserSession currentSession, String object, String userName) throws JasDBStorageException {
        throwNotSupported();
    }

    @Override
    public void deleteUser(UserSession currentSession, String userName) throws JasDBStorageException {
        throwNotSupported();
    }

    protected static void throwNotSupported() throws JasDBSecurityException {
        throw new JasDBSecurityException("User management not supported, install the service wrapper");
    }
}
