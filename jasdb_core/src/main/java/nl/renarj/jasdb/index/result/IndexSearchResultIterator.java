/*
 * The JASDB software and code is Copyright protected 2011 and owned by Renze de Vries
 * 
 * All the code and design principals in the codebase are also Copyright 2011 
 * protected and owned Renze de Vries. Any unauthorized usage of the code or the 
 * design and principals as in this code is prohibited.
 */
package nl.renarj.jasdb.index.result;

import nl.renarj.jasdb.index.keys.Key;
import nl.renarj.jasdb.index.keys.keyinfo.KeyNameMapper;

import java.util.Iterator;

public interface IndexSearchResultIterator extends Iterable<Key>, Iterator<Key> {
    /**
     * The amount of results found in the index
     * @return The amount of results
     */
	int size();

    /**
     * If the result is empty
     * @return True if the result is empty, False if not
     */
	boolean isEmpty();

    /**
     * Resets the index result iterator
     */
	void reset();

    /**
     * Maps from field names to key indexes
     * @return The key name mapper to map indexes to field names
     */
    KeyNameMapper getKeyNameMapper();
}