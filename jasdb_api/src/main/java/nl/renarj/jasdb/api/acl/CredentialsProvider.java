package nl.renarj.jasdb.api.acl;

import nl.renarj.jasdb.api.kernel.InitializableModule;
import nl.renarj.jasdb.api.metadata.User;
import nl.renarj.jasdb.core.exceptions.JasDBStorageException;

import java.util.List;

/**
 * @author Renze de Vries
 */
public interface CredentialsProvider extends InitializableModule {
    User getUser(String userName, String sourceHost, String password) throws JasDBStorageException;

    List<String> getUsers() throws JasDBStorageException;

    User addUser(String userName, String allowedHost, String contentKey, String password) throws JasDBStorageException;

    void deleteUser(String userName) throws JasDBStorageException;
}
