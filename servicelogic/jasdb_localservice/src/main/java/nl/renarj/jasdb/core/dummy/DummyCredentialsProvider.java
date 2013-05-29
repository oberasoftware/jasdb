package nl.renarj.jasdb.core.dummy;

import nl.renarj.jasdb.api.acl.CredentialsProvider;
import nl.renarj.jasdb.api.kernel.KernelContext;
import nl.renarj.jasdb.api.metadata.User;
import nl.renarj.jasdb.core.exceptions.JasDBStorageException;

import java.util.List;

/**
 * @author Renze de Vries
 */
public class DummyCredentialsProvider implements CredentialsProvider {
    @Override
    public User getUser(String userName, String sourceHost, String password) throws JasDBStorageException {
        DummyUserManager.throwNotSupported();
        return null;
    }

    @Override
    public List<String> getUsers() throws JasDBStorageException {
        DummyUserManager.throwNotSupported();
        return null;
    }

    @Override
    public User addUser(String userName, String allowedHost, String contentKey, String password) throws JasDBStorageException {
        DummyUserManager.throwNotSupported();
        return null;
    }

    @Override
    public void deleteUser(String userName) throws JasDBStorageException {
        DummyUserManager.throwNotSupported();
    }

    @Override
    public void initialize(KernelContext context) throws JasDBStorageException {

    }
}
