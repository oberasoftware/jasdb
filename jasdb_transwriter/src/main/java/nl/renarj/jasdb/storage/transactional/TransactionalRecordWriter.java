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
import nl.renarj.jasdb.core.exceptions.RuntimeJasDBException;
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
import java.util.Optional;
import java.util.function.Function;

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
        this.index.close();
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
            return writer.readRecord(() -> getRecordPointer(documentId));
        } catch(RecordNotFoundException e) {
            return new RecordResultImpl(-1, null, 0, RECORD_FLAG.EMPTY);
        }
    }

    @Override
    public void writeRecord(UUIDKey documentId, ClonableDataStream dataStream) throws JasDBStorageException {
        String recordContents = RecordStreamUtil.toString(dataStream);
        writer.writeRecord(recordContents, p -> {
            try {
                index.insertIntoIndex(documentId.cloneKey(false).addKey(keyInfo.getKeyNameMapper(), "RECORD_POINTER", new LongKey(p)));
            } catch(JasDBStorageException e) {
                try {
                    removeRecord(documentId);
                    index.flushIndex();
                } catch (JasDBStorageException e1) {
                    LOG.error("", e);
                }

                throw new RuntimeJasDBException("Unable to write record already exists", e);
            }
        });
    }

    @Override
    public void removeRecord(UUIDKey documentId) throws JasDBStorageException {
        writer.removeRecord(() -> getRecordPointer(documentId), p -> {
            try {
                index.removeFromIndex(documentId);
                index.flushIndex();
            } catch (JasDBStorageException e) {
                LOG.error("Unable to remove record from index", e);
            }
        });
    }

    private Optional<Long> getRecordPointer(UUIDKey documentId) {
        try {
            IndexSearchResultIterator resultIterator = index.searchIndex(new EqualsCondition(documentId), Index.NO_SEARCH_LIMIT);
            if (!resultIterator.isEmpty()) {
                Key key = resultIterator.next();

                LongKey longKey = (LongKey) key.getKey(keyInfo.getKeyNameMapper(), "RECORD_POINTER");

                return Optional.of(longKey.getKey());
            } else {
                throw new RecordNotFoundException("Unable to read record: " + documentId + ", could not be found");
            }
        } catch(JasDBStorageException e) {
            LOG.error("Unable lookup data record for document: {}", documentId);
        }

        return Optional.empty();
    }

    @Override
    public void updateRecord(UUIDKey documentId, ClonableDataStream dataStream) throws JasDBStorageException {
        long updatedDocumentPointer = writer.updateRecord(RecordStreamUtil.toString(dataStream), () -> getRecordPointer(documentId), (oldp, newp) -> {
            if(oldp != newp) {
                Key newKey = documentId.cloneKey(false).addKey(keyInfo.getKeyNameMapper(), "RECORD_POINTER", new LongKey(newp));
                try {
                    index.updateKey(documentId, newKey);
                    index.flushIndex();
                } catch (JasDBStorageException e) {
                    LOG.error("", e);
                }
            }
        });
    }

    public Index getIndex() {
        return index;
    }

    public void verify(Function<RecordResult, UUIDKey> f) {
        try {
            RecordIterator recordResults = writer.readAllRecords();
            for(RecordResult result : recordResults) {
                UUIDKey key = f.apply(result);
                boolean shouldUpdatePosition = false;
                try {
                    if (!readRecord(key).isRecordFound()) {
                        shouldUpdatePosition = true;
                    }
                } catch(DatastoreException e) {
                    shouldUpdatePosition = true;
                }

                if(shouldUpdatePosition) {
                    long p = ((RecordResultImpl) result).getRecordPointer();
                    LOG.debug("Missing index pointer: {}", p);
                    Key insertKey = key.cloneKey(false)
                            .addKey(keyInfo.getKeyNameMapper(), "RECORD_POINTER", new LongKey(p));

                    try {
                        index.insertIntoIndex(insertKey);
                    } catch(JasDBStorageException e) {
                        index.updateKey(key, insertKey);
                    }
                }
            }

        } catch (JasDBStorageException e) {
            e.printStackTrace();
        }
    }
}
