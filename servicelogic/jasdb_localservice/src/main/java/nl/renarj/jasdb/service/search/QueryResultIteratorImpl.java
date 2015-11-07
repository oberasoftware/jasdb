/*
 * The JASDB software and code is Copyright protected 2011 and owned by Renze de Vries
 * 
 * All the code and design principals in the codebase are also Copyright 2011 
 * protected and owned Renze de Vries. Any unauthorized usage of the code or the 
 * design and principals as in this code is prohibited.
 */
package nl.renarj.jasdb.service.search;

import nl.renarj.jasdb.api.SimpleEntity;
import nl.renarj.jasdb.api.query.QueryResult;
import nl.renarj.jasdb.core.exceptions.JasDBStorageException;
import nl.renarj.jasdb.core.exceptions.RuntimeJasDBException;
import nl.renarj.jasdb.core.storage.RecordResult;
import nl.renarj.jasdb.core.storage.RecordWriter;
import nl.renarj.jasdb.index.keys.Key;
import nl.renarj.jasdb.index.keys.KeyUtil;
import nl.renarj.jasdb.index.keys.impl.UUIDKey;
import nl.renarj.jasdb.index.result.IndexSearchResultIterator;
import nl.renarj.jasdb.index.result.SearchLimit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;

public class QueryResultIteratorImpl implements QueryResult {
	private Logger log = LoggerFactory.getLogger(QueryResultIteratorImpl.class);
	
	private IndexSearchResultIterator indexIterator;
	private SearchLimit limit;
	private int currentIndex = 0;
    private RecordWriter recordWriter;
	
	protected QueryResultIteratorImpl(IndexSearchResultIterator indexIterator, SearchLimit limit, RecordWriter recordWriter) {
		this.indexIterator = indexIterator;
		this.limit = limit;
        this.recordWriter = recordWriter;
	}
	
	@Override
	public boolean hasNext() {
		return indexIterator != null && indexIterator.hasNext() && !limit.isMaxReached(currentIndex);
	}

	@Override
	public SimpleEntity next() {
		if(hasNext()) {
			if(!limit.isMaxReached(currentIndex)) {
				currentIndex++;
				Key key = indexIterator.next();
				try {
//					long recordPointer = KeyUtil.getRecordPointer(indexIterator.getKeyNameMapper(), key);
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
	public Iterator<SimpleEntity> iterator() {
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
