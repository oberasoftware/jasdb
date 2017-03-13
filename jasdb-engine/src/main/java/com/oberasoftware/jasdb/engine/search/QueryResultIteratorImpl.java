/*
 * The JASDB software and code is Copyright protected 2011 and owned by Renze de Vries
 * 
 * All the code and design principals in the codebase are also Copyright 2011 
 * protected and owned Renze de Vries. Any unauthorized usage of the code or the 
 * design and principals as in this code is prohibited.
 */
package com.oberasoftware.jasdb.engine.search;

import com.oberasoftware.jasdb.api.session.Entity;
import com.oberasoftware.jasdb.core.SimpleEntity;
import com.oberasoftware.jasdb.api.session.query.QueryResult;
import com.oberasoftware.jasdb.api.exceptions.JasDBStorageException;
import com.oberasoftware.jasdb.api.exceptions.RuntimeJasDBException;
import com.oberasoftware.jasdb.api.storage.RecordResult;
import com.oberasoftware.jasdb.api.storage.RecordWriter;
import com.oberasoftware.jasdb.api.index.keys.Key;
import com.oberasoftware.jasdb.core.index.keys.KeyUtil;
import com.oberasoftware.jasdb.core.index.keys.UUIDKey;
import com.oberasoftware.jasdb.api.index.query.IndexSearchResultIterator;
import com.oberasoftware.jasdb.api.index.query.SearchLimit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;

public class QueryResultIteratorImpl implements QueryResult {
	private Logger log = LoggerFactory.getLogger(QueryResultIteratorImpl.class);
	
	private IndexSearchResultIterator indexIterator;
	private SearchLimit limit;
	private int currentIndex = 0;
    private RecordWriter<UUIDKey> recordWriter;
	
	protected QueryResultIteratorImpl(IndexSearchResultIterator indexIterator, SearchLimit limit, RecordWriter<UUIDKey> recordWriter) {
		this.indexIterator = indexIterator;
		this.limit = limit;
        this.recordWriter = recordWriter;
	}
	
	@Override
	public boolean hasNext() {
		return indexIterator != null && indexIterator.hasNext() && !limit.isMaxReached(currentIndex);
	}

	@Override
	public Entity next() {
		if(hasNext()) {
			if(!limit.isMaxReached(currentIndex)) {
				currentIndex++;
				Key key = indexIterator.next();
				try {
                    UUIDKey documentKey = KeyUtil.getDocumentKey(indexIterator.getKeyNameMapper(), key);

					RecordResult result = recordWriter.readRecord(documentKey);
					if(result.isRecordFound()) {
						return SimpleEntity.fromStream(result.getStream());
					} else {
						log.warn("Could not find record: {}", documentKey);
					}
				} catch(JasDBStorageException e) {
					throw new RuntimeJasDBException("Unable to iterate over query result, no record pointer found in index");
				}
			}
		}
		return null;
	}
	
	public long size() {
		return indexIterator.size();
	}

	@Override
	public void remove() {
		log.warn("Remove operation on ResultIterator not implemented");
		//not implemented
	}
	
	public void reset() {
		this.indexIterator.reset();
	}

	@Override
	public Iterator<Entity> iterator() {
		return this;
	}

    @Override
    public void close() {

    }

    @Override
    public boolean isClosed() {
        return false;
    }
}
