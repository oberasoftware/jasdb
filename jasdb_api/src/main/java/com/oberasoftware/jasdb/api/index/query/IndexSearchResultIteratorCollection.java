/*
 * The JASDB software and code is Copyright protected 2011 and owned by Renze de Vries
 * 
 * All the code and design principals in the codebase are also Copyright 2011 
 * protected and owned Renze de Vries. Any unauthorized usage of the code or the 
 * design and principals as in this code is prohibited.
 */
package com.oberasoftware.jasdb.api.index.query;

import com.oberasoftware.jasdb.api.index.keys.Key;

import java.util.List;

public interface IndexSearchResultIteratorCollection extends IndexSearchResultIterator {
    /**
     * Gets a list of keys contained in the iterator
     * @return THe list of keys in the iterator
     */
	List<Key> getKeys();

    /**
     * Gets a subset of the collection of keys in the iterator
     * @param start THe start of the subset
     * @param limit The maximum amount to put in the subset
     * @return The subset as list, empty list if nothing in iterator
     */
    List<Key> subset(int start, int limit);
}
