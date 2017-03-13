package com.oberasoftware.jasdb.api.security;

import com.oberasoftware.jasdb.api.exceptions.JasDBStorageException;
import com.oberasoftware.jasdb.api.model.GrantObject;
import com.oberasoftware.jasdb.api.model.User;

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
