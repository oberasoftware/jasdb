package com.oberasoftware.jasdb.api.session;

import com.oberasoftware.jasdb.api.security.AccessMode;
import com.oberasoftware.jasdb.api.exceptions.JasDBStorageException;

import java.util.List;

/**
 * @author Renze de Vries
 */
public interface UserAdministration {
    /**
     * Add a new user to the database, this user will be allowed to connected from the specified host or all hosts
     * if the wildcard '*' is used. Any user added to the database by default has read only access.
     *
     * @param username The name of the user
     * @param allowedHost The allowed host, '*' for wildcard
     * @param password The password of the user
     * @throws JasDBStorageException If unable to create the user due to insufficient permissions
     */
    void addUser(String username, String allowedHost, String password) throws JasDBStorageException;

    /**
     * Deletes a user from the database, does not revoke grants
     * @param username The name of the user
     * @throws JasDBStorageException If unable to delete the user due to insufficient permissions
     */
    void deleteUser(String username) throws JasDBStorageException;

    /**
     * Gets a list of all the users
     * @return The list of users
     * @throws JasDBStorageException If unable to get the list of users
     */
    List<String> getUsers() throws JasDBStorageException;

    /**
     * GrantObject the user access to the specified object with the given access level
     * @param username The username
     * @param object The object to grant permissions to
     * @param mode The mode of allowed access
     * @throws JasDBStorageException If unable to grant, for example with insufficient permissions
     */
    void grant(String username, String object, AccessMode mode) throws JasDBStorageException;

    /**
     * Revokes a grant for a given user
     * @param username The user to revoke the grant from
     * @param object The object to revoke on
     * @throws JasDBStorageException If unable to revoke the grant, for example with insufficient permissionse
     */
    void revoke(String username, String object) throws JasDBStorageException;

}
