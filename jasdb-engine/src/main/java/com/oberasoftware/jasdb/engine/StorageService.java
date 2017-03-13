package com.oberasoftware.jasdb.engine;

import com.oberasoftware.jasdb.api.exceptions.JasDBStorageException;
import com.oberasoftware.jasdb.api.session.Entity;
import com.oberasoftware.jasdb.api.session.query.QueryResult;
import com.oberasoftware.jasdb.core.context.RequestContext;
import com.oberasoftware.jasdb.api.index.query.SearchLimit;
import com.oberasoftware.jasdb.api.index.CompositeIndexField;
import com.oberasoftware.jasdb.api.index.IndexField;
import com.oberasoftware.jasdb.api.session.query.SortParameter;
import com.oberasoftware.jasdb.api.engine.Configuration;
import com.oberasoftware.jasdb.engine.query.operators.BlockOperation;

import java.util.List;

/**
 * The storage serivce is the main storage interface in JasDB, it handles all interactions against
 * a bag and related resources like indexes. All operations like create, read, update, delete and querying
 * go through this interface.
 */
public interface StorageService {
    /**
     * Gets the name of the bag that this storage service represents
     * @return The name of the bag
     */
    String getBagName();

    /**
     * Gets the instance this bag belongs to
     * @return The instance id
     */
    String getInstanceId();

    /**
     * Opens the service and all the resources required. This is guaranteed to be called
     * before any operations on the service are called.
     * @param configuration The configuration
     * @throws JasDBStorageException If unable to open the storage service
     */
	void openService(Configuration configuration) throws JasDBStorageException;

    /**
     * Closes the service and all its used resources
     * @throws JasDBStorageException If unable to cleanly close the resources
     */
	void closeService() throws JasDBStorageException;

    /**
     * An operation that can be called to guarantee all changes are flushed to the disk.
     * @throws JasDBStorageException If unable to flush the changes to the disk
     */
    void flush() throws JasDBStorageException;

    /**
     * Removes the bag and all related resources from the storage location
     * @throws JasDBStorageException If unable to remove the bag and related resources
     */
    void remove() throws JasDBStorageException;

	/**
	 * This inserts the entity into the storage and indexes
	 *
     * @param context The request context
     * @param entity The entity to store in the storage and which is used to populate the indexes
     * @throws JasDBStorageException If unable to insert the entity or the indexes
	 */
	void insertEntity(RequestContext context, Entity entity)	throws JasDBStorageException;

	/**
	 * This removes and entity from storage and the indexes
	 *
     *
     * @param context The request context
     * @param entity The entity to remove from the storage and the indexes
     * @throws JasDBStorageException If unable to remove the entity from the indexes or storage
	 */
	void removeEntity(RequestContext context, Entity entity)	throws JasDBStorageException;

    /**
     * This removes the entity based on the internal id of the entity
     * @param context The request context
     * @param internalId The entity to remove the storage and indexes
     * @throws JasDBStorageException If unable to remove the entity from the indexes or storage
     */
    void removeEntity(RequestContext context, String internalId) throws JasDBStorageException;

	/**
	 * This updates the entity in the storage and indexes if needed
	 *
     * @param context The request context
     * @param entity The entity to update
     * @throws JasDBStorageException If unable to update
	 */
	void updateEntity(RequestContext context, Entity entity) throws JasDBStorageException;

	/**
	 * This either creates or updates the entity
	 * @param context the request context
	 * @param entity the entity to persist
	 * @throws JasDBStorageException If unable to persist the entity
     */
	void persistEntity(RequestContext context, Entity entity) throws JasDBStorageException;

    /**
     * Gets the amount of items stored
     * @return The amount of stored items
     * @throws JasDBStorageException If unable to retrieve the amount of stored items
     */
	long getSize() throws JasDBStorageException;

    /**
     * Gets the size on the disk (if the flushing strategy is async could misrepresent the real number)
     * @return The disk size
     * @throws JasDBStorageException If unable to return the amount of used disk space
     */
	long getDiskSize() throws JasDBStorageException;
	
	/**
	 * Retrieve a specific entity from storage by the given unique document id
	 *
     * @param requestContext The request context
     * @param id The unique documentId of the item
     * @return The entity loaded from storage
	 * @throws JasDBStorageException If unable to retrieve the item from storage
	 */
	Entity getEntityById(RequestContext requestContext, String id) throws JasDBStorageException;

	/**
	 * This retrieves all items from storage with an iterator, the documents are only loaded once the iterator is used.
	 *
	 * @return The QueryResult for all records in storage
	 * @throws JasDBStorageException If unable to load the iterator over all documents
	 */
	QueryResult getEntities(RequestContext context) throws JasDBStorageException;

	/**
	 * This retrieves all items from storage with an iterator, the documents are only loaded 
	 * once the iterator is used. The iterator will only return a limited set of result based on the max specified.
	 * 
	 * @param max The maximum amount of records to retrieve
	 * @return The QueryResult for all records in storage
	 * @throws JasDBStorageException If unable to load the iterator over all documents
	 */
	QueryResult getEntities(RequestContext context, int max) throws JasDBStorageException;

	/**
	 * This is the query entrypoint, all API queries are translated into a Query Object Model which can be executed on the
	 * indexes and be sorted after all index queries have been resolved
     *
     * @param context THe request context
     * @param blockOperation The main blockoperation, the parent item in the Query Object Model.
     * @param limit The limits of the given query
     * @param params The sorting paramaters to be applied to the resultset
     * @return A Query ResultSet which can be iterated over, the records are only loaded when iterating.
	 * @throws JasDBStorageException If unable to execute the search query
	 */
	QueryResult search(RequestContext context, BlockOperation blockOperation, SearchLimit limit, List<SortParameter> params) throws JasDBStorageException;

    /**
     * Ensures an index with the given field is present. If the index is not present it will be created.
     * If the index is created it will block until index creation is completed.
     *
     * @param indexField The field to be indexed
     * @param isUnique True If the field needs to have a unique constraint, False if not
     * @param valueFields The value fields to be added to the index, can be used to improve sorting performance or secondary queries
     * @throws JasDBStorageException If unable to ensure the index exists
     */
    void ensureIndex(IndexField indexField, boolean isUnique, IndexField... valueFields) throws JasDBStorageException;

    /**
     * Ensures an index with a composite key (multiple fields) is present. If the index is not present it will be created.
     * If the index is created it will block until index creation is completed.
     *
     * @param indexField The indexed composite fields
     * @param isUnique True If the field needs to have a unique constraint, False if not
     * @param valueFields The value fields to be added to the index, can be used to improve sorting performance or secondary queries
     * @throws JasDBStorageException If unable to ensure the index exists
     */
    void ensureIndex(CompositeIndexField indexField, boolean isUnique, IndexField... valueFields) throws JasDBStorageException;

    /**
     * Gets a list of all present index names
     * @return The list of index names
     * @throws JasDBStorageException If unable to retrieve the list of index names
     */
    List<String> getIndexNames() throws JasDBStorageException;

    /**
     * Removes the index with the given name
     * @param indexName The name of the index to be removed
     * @throws JasDBStorageException If unable to remove the index
     */
    void removeIndex(String indexName) throws JasDBStorageException;
}
