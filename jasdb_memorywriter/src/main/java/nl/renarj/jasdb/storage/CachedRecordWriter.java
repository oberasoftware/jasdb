/*
 * The JASDB software and code is Copyright protected 2011 and owned by Renze de Vries
 * 
 * All the code and design principals in the codebase are also Copyright 2011 
 * protected and owned Renze de Vries. Any unauthorized usage of the code or the 
 * design and principals as in this code is prohibited.
 */
package nl.renarj.jasdb.storage;

import nl.renarj.core.caching.Bucket;
import nl.renarj.core.caching.CacheManager;
import nl.renarj.core.exceptions.CoreConfigException;
import nl.renarj.jasdb.core.exceptions.JasDBStorageException;
import nl.renarj.jasdb.core.storage.RecordIterator;
import nl.renarj.jasdb.core.storage.RecordResult;
import nl.renarj.jasdb.core.storage.RecordWriter;
import nl.renarj.jasdb.core.streams.ClonableDataStream;
import nl.renarj.jasdb.index.keys.impl.UUIDKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CachedRecordWriter implements RecordWriter {
	private Logger log = LoggerFactory.getLogger(CachedRecordWriter.class);
	private Bucket bucket;
	
	private RecordWriter wrappedWriter;
	private String storeName;
	private CacheManager cacheManager;
	
	public CachedRecordWriter(CacheManager cacheManager, String storeName, RecordWriter wrappedWriter) {
		this.cacheManager = cacheManager;
		this.wrappedWriter = wrappedWriter;
		this.storeName = storeName;
	}

	@Override
	public void openWriter() throws JasDBStorageException {
		try {
			bucket = cacheManager.getBucket("RecordBucket_" + storeName);
			
			wrappedWriter.openWriter();
		} catch(CoreConfigException e) {
			throw new JasDBStorageException("", e);
		}
	}

	@Override
	public void closeWriter() throws JasDBStorageException {
		wrappedWriter.closeWriter();
	}

    @Override
    public void flush() throws JasDBStorageException {
        wrappedWriter.flush();
    }

    @Override
	public boolean isOpen() {
		return wrappedWriter.isOpen();
	}

	@Override
	public RecordIterator readAllRecords() throws JasDBStorageException {
		return wrappedWriter.readAllRecords();
	}

	@Override
	public RecordIterator readAllRecords(int limit) throws JasDBStorageException {
		return wrappedWriter.readAllRecords(limit);
	}

	@Override
	public long getDiskSize() throws JasDBStorageException {
		return wrappedWriter.getDiskSize();
	}

	@Override
	public long getSize() throws JasDBStorageException {
		return wrappedWriter.getSize();
	}

    @Override
    public RecordResult readRecord(UUIDKey documentId) throws JasDBStorageException {
        String cachingKey = storeName + "_" + documentId.toString();
        if(!bucket.containsItem(cachingKey)) {
            return readRecordFromStore(cachingKey, documentId);
        } else {
            log.debug("Cache hit for record: {} key: {}", documentId, cachingKey);
            CachableRecord cachedRecord = (CachableRecord) bucket.getItem(cachingKey);
            if(cachedRecord != null) {
                return cachedRecord.getResult();
            } else {
                return readRecordFromStore(cachingKey, documentId);
            }
        }
    }

    private RecordResult readRecordFromStore(String cachingKey, UUIDKey key) throws JasDBStorageException {
        log.debug("Cache miss for record: {}", key);
        RecordResult result = wrappedWriter.readRecord(key);
        if(!result.isRecordFound()) {
            bucket.put(cachingKey, new CachableRecord(result));
        }

        return result;
    }


    @Override
    public void writeRecord(UUIDKey documentId, ClonableDataStream dataStream) throws JasDBStorageException {
        wrappedWriter.writeRecord(documentId, dataStream);

        String cachingKey = storeName + "_" + documentId.toString();
        if(bucket.containsItem(cachingKey)) {
            bucket.remove(cachingKey);
        }
    }

	@Override
	public void removeRecord(UUIDKey documentId) throws JasDBStorageException {
		wrappedWriter.removeRecord(documentId);

        String cachingKey = storeName + "_" + documentId.toString();
		bucket.remove(cachingKey);
	}

    @Override
    public void updateRecord(UUIDKey documentId, ClonableDataStream dataStream) throws JasDBStorageException {
        wrappedWriter.updateRecord(documentId, dataStream);

        String cachingKey = storeName + "_" + documentId.toString();
        bucket.remove(cachingKey);
    }
}
