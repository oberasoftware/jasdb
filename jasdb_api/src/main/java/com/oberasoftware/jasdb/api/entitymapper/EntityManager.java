package com.oberasoftware.jasdb.api.entitymapper;

import nl.renarj.jasdb.api.SimpleEntity;
import nl.renarj.jasdb.core.exceptions.JasDBStorageException;

/**
 * @author Renze de Vries
 */
public interface EntityManager {
    /**
     * Does a persist which can either be a create or update operation against the database.
     *
     * @param persistableObject The object to persist
     * @return The persisted JasDB object that was stored
     * @throws JasDBStorageException If unable to store the object
     */
    SimpleEntity persist(Object persistableObject) throws JasDBStorageException;

    /**
     * Removes the object from storage
     * @param persistableObject The object to remove from storage
     * @throws JasDBStorageException If unable to delete the object from storage
     */
    void remove(Object persistableObject) throws JasDBStorageException;
}
