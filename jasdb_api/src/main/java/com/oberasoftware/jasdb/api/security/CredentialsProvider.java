package com.oberasoftware.jasdb.api.security;

import com.oberasoftware.jasdb.api.model.User;
import com.oberasoftware.jasdb.api.exceptions.JasDBStorageException;

import java.util.List;

/**
 * @author Renze de Vries
 */
public interface CredentialsProvider {
    User getUser(String userName, String sourceHost, String password) throws JasDBStorageException;

    List<String> getUsers() throws JasDBStorageException;

    User addUser(String userName, String allowedHost, String contentKey, String password) throws JasDBStorageException;

    void deleteUser(String userName) throws JasDBStorageException;
}
