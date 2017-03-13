package com.oberasoftware.jasdb.api.entitymapper;

import com.oberasoftware.jasdb.api.exceptions.JasDBStorageException;
import com.oberasoftware.jasdb.api.session.Entity;
import com.oberasoftware.jasdb.api.session.query.QueryBuilder;

import java.util.List;

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
    Entity persist(Object persistableObject) throws JasDBStorageException;

    /**
     * Removes the object from storage
     * @param persistableObject The object to remove from storage
     * @throws JasDBStorageException If unable to delete the object from storage
     */
    void remove(Object persistableObject) throws JasDBStorageException;

    /**
     * Finds an entity by the identifier
     * @param type The target entity type you are trying to load
     * @param entityId The identifier of the entity
     * @param <T> The generic entity type
     * @return The loaded entity if found, null if not found
     * @throws JasDBStorageException If unable to load entity from storage or unable to map to target type
     */
    <T> T findEntity(Class<T> type, String entityId) throws JasDBStorageException;

    /**
     *
     * @param type The target entity type you are trying to load
     * @param builder The query
     * @param <T> The generic entity type
     * @return The list of found entities matching the query
     * @throws JasDBStorageException If unable to load entities from storage or unable to map to target type
     */
    <T> List<T> findEntities(Class<T> type, QueryBuilder builder) throws JasDBStorageException;

    /**
     *
     * @param type The target entity type you are trying to load
     * @param builder The query
     * @param limit The maximum amount of entities to load
     * @param <T> The generic entity type
     * @return The list of found entities matching the query
     * @throws JasDBStorageException If unable to load entities from storage or unable to map to target type
     */
    <T> List<T> findEntities(Class<T> type, QueryBuilder builder, int limit) throws JasDBStorageException;

    /**
     *
     * @param type The target entity type you are trying to load
     * @param builder The query
     * @param start The starting pagination page
     * @param limit The maximum amount of entities to load
     * @param <T> The generic entity type
     * @return The list of found entities matching the query
     * @throws JasDBStorageException If unable to load entities from storage or unable to map to target type
     */
    <T> List<T> findEntities(Class<T> type, QueryBuilder builder, int start, int limit) throws JasDBStorageException;
}
