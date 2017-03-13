/*
 * The JASDB software and code is Copyright protected 2011 and owned by Renze de Vries
 * 
 * All the code and design principals in the codebase are also Copyright 2011 
 * protected and owned Renze de Vries. Any unauthorized usage of the code or the 
 * design and principals as in this code is prohibited.
 */
package com.oberasoftware.jasdb.api.session;

import com.oberasoftware.jasdb.api.exceptions.JasDBStorageException;
import com.oberasoftware.jasdb.api.index.CompositeIndexField;
import com.oberasoftware.jasdb.api.index.IndexField;
import com.oberasoftware.jasdb.api.session.query.*;

import java.util.List;

/**
 * The Entity bag is the main API for all interactions onto the Bag. Every bag contains a number
 * of indexes. The EntityBag allows insertion, updates and removal operation of the entities
 * inside the bag.
 *
 * Also an extensive Query API is available for building queries onto the Bag.
 *
 * @author Renze de Vries
 *
 */
public interface EntityBag {

	/**
	 * Returns the name of the bag containing all data
	 * @return The name of the bag containing all data
	 */
	String getName() throws JasDBStorageException;
	
	/**
	 * Returns the amount of entities in the bag
	 * @return The amount of entities in the bag
	 */
	long getSize() throws JasDBStorageException;
	
	/**
	 * Returns the size on the disk of the entities
	 * @return The size on the disk
	 */
	long getDiskSize() throws JasDBStorageException;

    /**
     * Forcibly flushes all the data in the bag to the storage
     * @throws JasDBStorageException If unable to flush the bag
     */
    void flush() throws JasDBStorageException;

	/**
	 * Adds an entity to the bag of entities
	 * @param entity The entity to add to the bag
	 * @return THe persisted entity
	 * @throws JasDBStorageException If unable to persist the entity into the bag
	 */
	Entity addEntity(Entity entity) throws JasDBStorageException;
	
	/**
	 * Updates an entity in the bag of entities
	 * @param entity The entity to update in the bag
	 * @return The updated entity
	 * @throws JasDBStorageException If unable to update the entity in the bag
	 */
	Entity updateEntity(Entity entity) throws JasDBStorageException;

	/**
	 * Persists the provided entity, if not exists will be created, if already exists it will be updated
	 * @param entity The entity to be persisted
	 * @return The persisted entity
	 * @throws JasDBStorageException If unable to persist the entity
     */
	Entity persist(Entity entity) throws JasDBStorageException;

    /**
     * Remove the entity from the bag
     * @param entity The entity to be removed
     *
     * @throws JasDBStorageException If unable to remove the entity from the bag
     */
    void removeEntity(Entity entity) throws JasDBStorageException;

    /**
     * Removes the entity from the bag using the id
     * @param entityId The id of the entity to delete
     * @throws JasDBStorageException If unable to remove the entity from the bag
     */
    void removeEntity(String entityId) throws JasDBStorageException;
	
	/**
	 * Ensures there is an index present on a given field in this bag, will create if not existent, will do nothing
	 * if index already exists.
	 * 
	 * @param indexField The field to index
	 * @param isUnique True if the field contains unique values, False if not
	 * @param valueFields The valueFields to be stored in the index
	 * @throws JasDBStorageException If unable to ensure the index
	 */
	void ensureIndex(IndexField indexField, boolean isUnique, IndexField... valueFields) throws JasDBStorageException;

    /**
     * Removes the index from the bag
     * @param indexKeyName The name of the index
     * @throws JasDBStorageException If unable to cleanly remove the index
     */
    void removeIndex(String indexKeyName) throws JasDBStorageException;

	/**
	 * Ensures there is an index present on a given amount of fields in this bag, will create if not existent, will do nothing
	 * if index already exists.
	 * 
	 * @param indexField The fields to persist in the index
	 * @param isUnique True if the field contains unique values, False if not
	 * @param valueFields The valueFields to be stored in the index
	 * @throws JasDBStorageException If unable to ensure the index
	 */
	void ensureIndex(CompositeIndexField indexField, boolean isUnique, IndexField... valueFields) throws JasDBStorageException;

    /**
     * Gets a list of all the index names on this bag
     * @return The list of all the index names
     * @throws JasDBStorageException If unable to retrieve the indexes
     */
    List<String> getIndexNames() throws JasDBStorageException;
	
	/**
	 * Builds a query for document in the storage for a specific queryfield with optional sorting parameters
	 * 
	 * @param queryField The field to query
	 * @param params The sorting parameters
	 * @return The QueryExecutor which can execute the query
	 * @throws JasDBStorageException If unable to build a document query
	 */
	QueryExecutor find(QueryField queryField, SortParameter... params) throws JasDBStorageException;

	/**
	 * Builds a query for document in the storage for multiple queryfields with optional sorting parameters
	 * 
	 * @param queryFields The fields to query
	 * @param params The sorting parameters
	 * @return The QueryExecutor which can execute the query
	 * @throws JasDBStorageException If unable to build a document query
	 */
	QueryExecutor find(CompositeQueryField queryFields, SortParameter... params) throws JasDBStorageException;

	/**
	 * Builds a query for document in the storage using the QueryBuilder which has a fluent query building mechanism.
	 * 
	 * @param queryBuilder The querybuilder to use for building the query
	 * @return The QueryExecutor which can execute the query
	 * @throws JasDBStorageException If unable to build a document query
	 */
	QueryExecutor find(QueryBuilder queryBuilder) throws JasDBStorageException;
	
	/**
	 * Execute a query returning all records in the bag
	 * @return The QueryResult iterator for all records in the bag
	 * @throws JasDBStorageException If unable to return a all records iterator for this bag
	 */
	QueryResult getEntities() throws JasDBStorageException;

	/**
	 * Execute a query returning all records in the bag with a given max
	 * 
	 * @param max The maximum amount of records to return
	 * @return The QueryResult iterator for all records in the bag
	 * @throws JasDBStorageException If unable to return a all records iterator for this bag
	 */
	QueryResult getEntities(int max) throws JasDBStorageException;

	/**
	 * Retrieves a specific entity from the bag
	 * @param entityId The entityId to retrieve the document for
	 * @return The entity found in the bag
	 * @throws JasDBStorageException If unable to find the entity in the bag
	 */
	Entity getEntity(String entityId) throws JasDBStorageException;
}