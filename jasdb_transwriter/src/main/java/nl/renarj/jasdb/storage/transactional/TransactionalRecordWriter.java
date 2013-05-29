/*
 * The JASDB software and code is Copyright protected 2011 and owned by Renze de Vries
 * 
 * All the code and design principals in the codebase are also Copyright 2011 
 * protected and owned Renze de Vries. Any unauthorized usage of the code or the 
 * design and principals as in this code is prohibited.
 */
package nl.renarj.jasdb.storage.transactional;

import nl.renarj.jasdb.core.exceptions.DatastoreException;
import nl.renarj.jasdb.core.exceptions.JasDBStorageException;
import nl.renarj.jasdb.core.storage.RecordIterator;
import nl.renarj.jasdb.core.storage.RecordResult;
import nl.renarj.jasdb.core.storage.RecordWriter;
import nl.renarj.jasdb.core.streams.ClonableDataStream;
import nl.renarj.jasdb.core.utils.FileUtils;
import nl.renarj.jasdb.index.Index;
import nl.renarj.jasdb.index.btreeplus.BTreeIndex;
import nl.renarj.jasdb.index.keys.Key;
import nl.renarj.jasdb.index.keys.impl.LongKey;
import nl.renarj.jasdb.index.keys.impl.UUIDKey;
import nl.renarj.jasdb.index.keys.keyinfo.KeyInfo;
import nl.renarj.jasdb.index.keys.keyinfo.KeyInfoImpl;
import nl.renarj.jasdb.index.keys.types.LongKeyType;
import nl.renarj.jasdb.index.keys.types.UUIDKeyType;
import nl.renarj.jasdb.index.result.IndexSearchResultIterator;
import nl.renarj.jasdb.index.search.EqualsCondition;
import nl.renarj.jasdb.index.search.IndexField;
import nl.renarj.jasdb.storage.RecordStreamUtil;
import nl.renarj.jasdb.storage.exceptions.RecordNotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

public class TransactionalRecordWriter implements RecordWriter {
    private static final Logger LOG = LoggerFactory.getLogger(TransactionalRecordWriter.class);


    private File indexLocation;
    private KeyInfo keyInfo;
    private Index index;

    private Writer writer;

	public TransactionalRecordWriter(File recordLocation) {
        this.indexLocation = new File(FileUtils.removeExtension(recordLocation.toString()) + ".idx");
        this.writer = new FSWriter(recordLocation);
	}

	@Override
	public void openWriter() throws JasDBStorageException {
        this.keyInfo = new KeyInfoImpl(new IndexField("__ID", new UUIDKeyType()), new IndexField("RECORD_POINTER", new LongKeyType()));
        this.index = new BTreeIndex(indexLocation, keyInfo);

        this.writer.openWriter();
        this.index.openIndex();
	}

	@Override
	public void closeWriter() throws JasDBStorageException {
        this.writer.closeWriter();
        this.index.closeIndex();
	}

    @Override
    public void flush() throws JasDBStorageException {
        index.flushIndex();
    }

	@Override
	public boolean isOpen() {
		return this.writer.isOpen();
	}
	
	@Override
	public long getDiskSize() throws JasDBStorageException {
        return this.writer.getDiskSize();
    }

	@Override
	public long getSize() {
        return this.writer.getSize();
	}

	@Override
	public RecordIterator readAllRecords() throws DatastoreException {
		return readAllRecords(-1);
	}

	@Override
	public RecordIterator readAllRecords(int limit) throws DatastoreException {
        return writer.readAllRecords(limit);
	}

    @Override
    public RecordResult readRecord(UUIDKey documentId) throws JasDBStorageException {
        try {
            long recordPointer = getRecordPointer(documentId);

            RecordResultImpl recordResult = writer.readRecord(recordPointer);

            return recordResult;
        } catch(RecordNotFoundException e) {
            return new RecordResultImpl(-1, null, 0, RECORD_FLAG.EMPTY);
        }
    }

    @Override
    public void writeRecord(UUIDKey documentId, ClonableDataStream dataStream) throws JasDBStorageException {
        String recordContents = RecordStreamUtil.toString(dataStream);
        long recordPointer = writer.writeRecord(recordContents);

        try {
            index.insertIntoIndex(documentId.cloneKey(false).addKey(keyInfo.getKeyNameMapper(), "RECORD_POINTER", new LongKey(recordPointer)));
        } catch(JasDBStorageException e) {
            writer.removeRecord(recordPointer);

            LOG.error("Unable to write into index, removing written record", e);
        }
    }

    @Override
    public void removeRecord(UUIDKey documentId) throws JasDBStorageException {
        writer.removeRecord(getRecordPointer(documentId));
        index.removeFromIndex(documentId);
    }

    private long getRecordPointer(UUIDKey documentId) throws JasDBStorageException {
        IndexSearchResultIterator resultIterator = index.searchIndex(new EqualsCondition(documentId), Index.NO_SEARCH_LIMIT);
        if(!resultIterator.isEmpty()) {
            Key key = resultIterator.next();

            LongKey longKey = (LongKey) key.getKey(keyInfo.getKeyNameMapper(), "RECORD_POINTER");

            return longKey.getKey();
        } else {
            throw new RecordNotFoundException("Unable to read record: " + documentId + ", could not be found");
        }
    }

    @Override
    public void updateRecord(UUIDKey documentId, ClonableDataStream dataStream) throws JasDBStorageException {
        long currentDocumentPointer = getRecordPointer(documentId);

        long updatedDocumentPointer = writer.updateRecord(RecordStreamUtil.toString(dataStream), currentDocumentPointer);
        if(currentDocumentPointer != updatedDocumentPointer) {
            Key newKey = documentId.cloneKey(false).addKey(keyInfo.getKeyNameMapper(), "RECORD_POINTER", new LongKey(updatedDocumentPointer));
            index.updateKey(documentId, newKey);
        }
    }


}
