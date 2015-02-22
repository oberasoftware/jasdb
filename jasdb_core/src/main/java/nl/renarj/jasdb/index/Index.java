/*
 * The JASDB software and code is Copyright protected 2011 and owned by Renze de Vries
 * 
 * All the code and design principals in the codebase are also Copyright 2011 
 * protected and owned Renze de Vries. Any unauthorized usage of the code or the 
 * design and principals as in this code is prohibited.
 */
package nl.renarj.jasdb.index;

import nl.renarj.core.utilities.configuration.Configuration;
import nl.renarj.jasdb.core.IndexableItem;
import nl.renarj.jasdb.core.caching.MemoryAware;
import nl.renarj.jasdb.core.exceptions.ConfigurationException;
import nl.renarj.jasdb.core.exceptions.JasDBStorageException;
import nl.renarj.jasdb.index.keys.Key;
import nl.renarj.jasdb.index.keys.keyinfo.KeyInfo;
import nl.renarj.jasdb.index.result.IndexSearchResultIteratorCollection;
import nl.renarj.jasdb.index.result.SearchLimit;
import nl.renarj.jasdb.index.search.SearchCondition;

import java.util.Iterator;
import java.util.Set;

/**
 * This is the index interface, any Index implementation will require this interface to be implemented. Any
 * index will support search, insert, update and remove operations on the index. The index will work on a given
 * Key object which can be used for any operation on the index. Any index supplies a KeyInfo object which can be
 * used to determine information about the Index. 
 * 
 * @author Renze de Vries
 *
 */
public interface Index extends AutoCloseable {
    SearchLimit NO_SEARCH_LIMIT = new SearchLimit();

	/**
	 * Configure the index with configuration settings found in global configuration
	 * 
	 * @param configuration The configuration object
	 * @throws ConfigurationException If unable to configure the index
	 */
	void configure(Configuration configuration) throws ConfigurationException;
	
	/**
	 * This returns all global key information being stored in this index
	 * @return The key information of the index
	 */
	KeyInfo getKeyInfo();

    String getName();
	
	int getPageSize();
    
    int getIndexType();

    /**
     * Returns the amount of keys in the index
     * @return The amount of keys in the index
     */
    long count();

    boolean hasUniqueConstraint();
	
	/**
 	 * Determines how close the gives fields match the index descriptor. The scale is on 0-200. This would
	 * reach 200 in case all value fields and index fields are a 100% match.
	 * The calculation is N (number of matching fields) / field.size + (N / fields.size) * included matching
	 * columns.
	 *   	
	 * @param fields The fields being requested
	 * @return The match ratio from 0-200
 	*/
	int match(Set<String> fields);
	
	/**
	 * Do a search operation of the index given the searchcondition. The searchCondition is based on a specific search in this
	 * index and only applies to the given index. The searchlimit determines the maximum results to be retrieved.
	 * 
	 * @param searchCondition The searchcondition to apply to only this index
	 * @param searchLimit The searchlimit to apply whilst searching
	 * @return An iterator that will be able to iterate over results and is mergable
	 * @throws nl.renarj.jasdb.core.exceptions.JasDBStorageException If unable to complete the search in the index
	 */
	IndexSearchResultIteratorCollection searchIndex(SearchCondition searchCondition, SearchLimit searchLimit) throws JasDBStorageException;

    /**
     * This gets an iterator which allows iterating over the full index collection
     * @return The index iterator
     * @throws JasDBStorageException If unable to open the index iterator
     */
    IndexIterator getIndexIterator() throws JasDBStorageException;
	
	/**
	 * Inserts the given key into the index, the key is to expected to conform to the index key specification as determined
	 * in the keyinfo. 
	 * 
	 * @param key The key to be persisted to the index
	 * @throws nl.renarj.jasdb.core.exceptions.JasDBStorageException If unable to persist the key in the index
	 */
	void insertIntoIndex(Key key) throws JasDBStorageException;
	
	/**
	 * Remove the given key and its values from the index. The key only requires the key part to be populated, value fields
	 * are not required to be populated.
	 * 
	 * @param key The key to be removed from the index
	 * @throws nl.renarj.jasdb.core.exceptions.JasDBStorageException If unable to remove the key from the index
	 */
	void removeFromIndex(Key key) throws JasDBStorageException;
	
	/**
	 * Update the key payload inside the index, the key is to expected to conform to the index key specification as determined
	 * in the keyinfo. 
	 * 
	 * @param oldKey The previous key present in the index
     * @param newKey The new key value to be used instead
	 * @throws nl.renarj.jasdb.core.exceptions.JasDBStorageException If unable to update the key in the index
	 */
	void updateKey(Key oldKey, Key newKey) throws JasDBStorageException;

    /**
     * Opens the index and its resources
     * @throws nl.renarj.jasdb.core.exceptions.JasDBStorageException
     */
	void openIndex() throws JasDBStorageException;
	
	/**
	 * Close the index and all its used resources
	 * @throws nl.renarj.jasdb.core.exceptions.JasDBStorageException If unable to cleanly close the index
	 */
	void close() throws JasDBStorageException;

    /**
     * Removes the index and all used resources
     * @throws nl.renarj.jasdb.core.exceptions.JasDBStorageException If unable to delete the index
     */
    void removeIndex() throws JasDBStorageException;
	
	/**
	 * This persists the changes to the disk backing of the index
	 * @throws nl.renarj.jasdb.core.exceptions.JasDBStorageException If unable to flush the changes
	 */
	void flushIndex() throws JasDBStorageException;

    /**
     * This performs a scan of the index and reports on the integrity. The scan is done with an
     * intent indicating a rescan is requested or just a report. In case of a report it returns
     * the last scan result if present, if not present scan will be performed and returned.
     *
     * The scan operation will modify the index state according to scan report, if scan reveals invalid index
     * it will set state to invalid, etc.
     *
     *
     * @param intent The scanning intent, rescan or report
     * @param scanItems The list of items to scan for in the index
     * @return The scan report indicating index state
     * @throws nl.renarj.jasdb.core.exceptions.JasDBStorageException If unable to perform or complete index scan
     */
    IndexScanReport scan(ScanIntent intent, Iterator<IndexableItem> scanItems) throws JasDBStorageException;

    /**
     * Returns the state of the index
     * @return The state of index
     */
    IndexState getState();

	/**
	 * Do a full rebuild of the index and fully populate it based on provided item iterator
	 * 
	 * @param indexableItems The items to be indexed
	 * @throws nl.renarj.jasdb.core.exceptions.JasDBStorageException If unable to rebuild the index
	 */
	void rebuildIndex(Iterator<IndexableItem> indexableItems) throws JasDBStorageException;
	
	/**
	 * This is the memory manager of the index, this is used to determine vital memory parameters.
	 * 
	 * @return THe memory manager used by this index
	 */
	MemoryAware getMemoryManager() throws JasDBStorageException;
}
