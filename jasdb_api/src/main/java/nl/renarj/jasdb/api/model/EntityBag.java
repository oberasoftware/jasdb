/*
 * The JASDB software and code is Copyright protected 2011 and owned by Renze de Vries
 * 
 * All the code and design principals in the codebase are also Copyright 2011 
 * protected and owned Renze de Vries. Any unauthorized usage of the code or the 
 * design and principals as in this code is prohibited.
 */
package nl.renarj.jasdb.api.model;

import nl.renarj.jasdb.api.SimpleEntity;
import nl.renarj.jasdb.api.query.CompositeQueryField;
import nl.renarj.jasdb.api.query.QueryBuilder;
import nl.renarj.jasdb.api.query.QueryExecutor;
import nl.renarj.jasdb.api.query.QueryField;
import nl.renarj.jasdb.api.query.QueryResult;
import nl.renarj.jasdb.api.query.SortParameter;
import nl.renarj.jasdb.core.exceptions.JasDBStorageException;
import nl.renarj.jasdb.index.search.CompositeIndexField;
import nl.renarj.jasdb.index.search.IndexField;

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
	 * @throws nl.renarj.jasdb.core.exceptions.JasDBStorageException If unable to persist the entity into the bag
	 */
	SimpleEntity addEntity(SimpleEntity entity) throws JasDBStorageException;
	
	/**
	 * Updates an entity in the bag of entities
	 * @param entity The entity to update in the bag
	 * @return The updated entity
	 * @throws nl.renarj.jasdb.core.exceptions.JasDBStorageException If unable to update the entity in the bag
	 */
	SimpleEntity updateEntity(SimpleEntity entity) throws JasDBStorageException;


    /**
     * Remove the entity from the bag
     * @param entity The entity to be removed
     *
     * @throws nl.renarj.jasdb.core.exceptions.JasDBStorageException If unable to remove the entity from the bag
     */
    void removeEntity(SimpleEntity entity) throws JasDBStorageException;

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
	 * @throws nl.renarj.jasdb.core.exceptions.JasDBStorageException If unable to ensure the index
	 */
	void ensureIndex(IndexField indexField, boolean isUnique, IndexField... valueFields) throws JasDBStorageException;

    /**
     * Removes the index from the bag
     * @param indexKeyName The name of the index
     * @throws nl.renarj.jasdb.core.exceptions.JasDBStorageException If unable to cleanly remove the index
     */
    void removeIndex(String indexKeyName) throws JasDBStorageException;

	/**
	 * Ensures there is an index present on a given amount of fields in this bag, will create if not existent, will do nothing
	 * if index already exists.
	 * 
	 * @param indexField The fields to persist in the index
	 * @param isUnique True if the field contains unique values, False if not
	 * @param valueFields The valueFields to be stored in the index
	 * @throws nl.renarj.jasdb.core.exceptions.JasDBStorageException If unable to ensure the index
	 */
	void ensureIndex(CompositeIndexField indexField, boolean isUnique, IndexField... valueFields) throws JasDBStorageException;

    /**
     * Gets a list of all the index names on this bag
     * @return The list of all the index names
     * @throws nl.renarj.jasdb.core.exceptions.JasDBStorageException If unable to retrieve the indexes
     */
    public List<String> getIndexNames() throws JasDBStorageException;
	
	/**
	 * Builds a query for document in the storage for a specific queryfield with optional sorting parameters
	 * 
	 * @param queryField The field to query
	 * @param params The sorting parameters
	 * @return The QueryExecutor which can execute the query
	 * @throws nl.renarj.jasdb.core.exceptions.JasDBStorageException If unable to build a document query
	 */
	public QueryExecutor find(QueryField queryField, SortParameter... params) throws JasDBStorageException;

	/**
	 * Builds a query for document in the storage for multiple queryfields with optional sorting parameters
	 * 
	 * @param queryFields The fields to query
	 * @param params The sorting parameters
	 * @return The QueryExecutor which can execute the query
	 * @throws nl.renarj.jasdb.core.exceptions.JasDBStorageException If unable to build a document query
	 */
	public QueryExecutor find(CompositeQueryField queryFields, SortParameter... params) throws JasDBStorageException;

	/**
	 * Builds a query for document in the storage using the QueryBuilder which has a fluent query building mechanism.
	 * 
	 * @param queryBuilder The querybuilder to use for building the query
	 * @return The QueryExecutor which can execute the query
	 * @throws nl.renarj.jasdb.core.exceptions.JasDBStorageException If unable to build a document query
	 */
	public QueryExecutor find(QueryBuilder queryBuilder) throws JasDBStorageException;
	
	/**
	 * Execute a query returning all records in the bag
	 * @return The QueryResult iterator for all records in the bag
	 * @throws nl.renarj.jasdb.core.exceptions.JasDBStorageException If unable to return a all records iterator for this bag
	 */
	public QueryResult getEntities() throws JasDBStorageException;

	/**
	 * Execute a query returning all records in the bag with a given max
	 * 
	 * @param max The maximum amount of records to return
	 * @return The QueryResult iterator for all records in the bag
	 * @throws nl.renarj.jasdb.core.exceptions.JasDBStorageException If unable to return a all records iterator for this bag
	 */
	public QueryResult getEntities(int max) throws JasDBStorageException;

	/**
	 * Retrieves a specific entity from the bag
	 * @param entityId The entityId to retrieve the document for
	 * @return The entity found in the bag
	 * @throws nl.renarj.jasdb.core.exceptions.JasDBStorageException If unable to find the entity in the bag
	 */
	public SimpleEntity getEntity(String entityId) throws JasDBStorageException;
}