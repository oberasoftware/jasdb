/*
 * The JASDB software and code is Copyright protected 2011 and owned by Renze de Vries
 * 
 * All the code and design principals in the codebase are also Copyright 2011 
 * protected and owned Renze de Vries. Any unauthorized usage of the code or the 
 * design and principals as in this code is prohibited.
 */
package nl.renarj.jasdb.storage.query;

import nl.renarj.jasdb.api.SimpleEntity;
import nl.renarj.jasdb.api.query.QueryResult;
import nl.renarj.jasdb.core.exceptions.JasDBStorageException;
import nl.renarj.jasdb.core.storage.RecordIterator;
import nl.renarj.jasdb.core.storage.RecordResult;
import nl.renarj.jasdb.service.BagOperationUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;

public class StorageRecordIterator implements QueryResult {
    private static final Logger LOG = LoggerFactory.getLogger(StorageRecordIterator.class);

	private RecordIterator recordIterator;
    private long size;
	
	public StorageRecordIterator(long size, RecordIterator recordIterator) {
        this.size = size;
		this.recordIterator = recordIterator;
	}
	
	@Override
	public boolean hasNext() {
		return this.recordIterator.hasNext();
	}

	@Override
	public SimpleEntity next() {
		RecordResult result = recordIterator.next();
        try {

            if(result.isRecordFound()) {
                return BagOperationUtil.toEntity(result.getStream());
            } else {
                return null;
            }
        } catch(JasDBStorageException e) {
            LOG.error("", e);
            return null;
        }
	}

	@Override
	public void remove() {
		//nothing
	}

	public long size() {
		return size;
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
