/*
 * The JASDB software and code is Copyright protected 2011 and owned by Renze de Vries
 * 
 * All the code and design principals in the codebase are also Copyright 2011 
 * protected and owned Renze de Vries. Any unauthorized usage of the code or the 
 * design and principals as in this code is prohibited.
 */
package com.oberasoftware.jasdb.core.index.query;

import com.oberasoftware.jasdb.api.index.keys.Key;
import com.oberasoftware.jasdb.api.index.keys.KeyNameMapper;
import com.oberasoftware.jasdb.api.index.query.IndexSearchResultIteratorCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public class IndexSearchResultIteratorImpl implements IndexSearchResultIteratorCollection {
	private static final Logger LOG = LoggerFactory.getLogger(IndexSearchResultIteratorImpl.class);
	
	private List<Key> keys;

	private Iterator<Key> keyIterator;

    private KeyNameMapper keyNameMapper;
	
	public IndexSearchResultIteratorImpl(List<Key> keys, KeyNameMapper keyNameMapper) {
		this.keys = keys;
		this.keyIterator = keys.iterator();
        this.keyNameMapper = keyNameMapper;
	}

    @Override
    public List<Key> subset(int start, int limit) {
        int endIndex = start + limit;
        if(keys.size() < endIndex) {
            endIndex = keys.size();
        }

        if(start < endIndex) {
        	return new ArrayList<>(keys.subList(start, endIndex));
        } else {
        	return Collections.emptyList();
        }        
    }

    @Override
	public List<Key> getKeys() {
		return this.keys;
	}

    @Override
    public KeyNameMapper getKeyNameMapper() {
        return keyNameMapper;
    }

    @Override
	public int size() {
		return this.keys.size();
	}
	
	/* (non-Javadoc)
	 * @see nl.renarj.pojodb.indexing.btree.IndexSearchResultIterator#isEmpty()
	 */
	@Override
	public boolean isEmpty() {
		return this.keys.isEmpty();
	}
	
	@Override
	public boolean hasNext() {
		return keyIterator.hasNext();
	}

	@Override
	public Key next() {
		return hasNext() ? keyIterator.next() : null;
	}

	@Override
	public void remove() {
        //not implemented
		LOG.warn("Remove operation on ResultIterator not implemented");
	}
	
	/* (non-Javadoc)
	 * @see nl.renarj.pojodb.indexing.btree.IndexSearchResultIterator#reset()
	 */
	@Override
	public void reset() {
		this.keyIterator = keys.iterator();
	}

	/* (non-Javadoc)
	 * @see nl.renarj.pojodb.indexing.btree.IndexSearchResultIterator#iterator()
	 */
	@Override
	public Iterator<Key> iterator() {
		return this;
	}
}
