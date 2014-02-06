package nl.renarj.jasdb.storage.btree;

import nl.renarj.core.statistics.StatRecord;
import nl.renarj.core.statistics.StatisticsMonitor;
import nl.renarj.jasdb.core.exceptions.JasDBStorageException;
import nl.renarj.jasdb.core.storage.RecordIterator;
import nl.renarj.jasdb.core.storage.RecordResult;
import nl.renarj.jasdb.core.storage.RecordWriter;
import nl.renarj.jasdb.core.streams.ClonableDataStream;
import nl.renarj.jasdb.index.Index;
import nl.renarj.jasdb.index.IndexIterator;
import nl.renarj.jasdb.index.IndexState;
import nl.renarj.jasdb.index.btreeplus.BTreeIndex;
import nl.renarj.jasdb.index.keys.Key;
import nl.renarj.jasdb.index.keys.impl.DataKey;
import nl.renarj.jasdb.index.keys.impl.UUIDKey;
import nl.renarj.jasdb.index.keys.keyinfo.KeyInfo;
import nl.renarj.jasdb.index.keys.keyinfo.KeyInfoImpl;
import nl.renarj.jasdb.index.keys.types.DataKeyType;
import nl.renarj.jasdb.index.keys.types.UUIDKeyType;
import nl.renarj.jasdb.index.result.IndexSearchResultIterator;
import nl.renarj.jasdb.index.search.EqualsCondition;
import nl.renarj.jasdb.index.search.IndexField;
import nl.renarj.jasdb.storage.exceptions.RecordStoreInUseException;

import java.io.File;
import java.nio.channels.OverlappingFileLockException;

/**
 * @author Renze de Vries
 */
public class BtreeIndexRecordWriter implements RecordWriter {
    private File recordStorageLocation;
    private Index index;
    private KeyInfo keyInfo;

    public BtreeIndexRecordWriter(File recordStorageLocation) {
        this.recordStorageLocation = recordStorageLocation;
    }

    @Override
    public long getDiskSize() throws JasDBStorageException {
        return recordStorageLocation.length();
    }

    @Override
    public long getSize() throws JasDBStorageException {
        return index.count();
    }

    @Override
    public void openWriter() throws JasDBStorageException {
        this.keyInfo = new KeyInfoImpl(new IndexField("__ID", new UUIDKeyType()), new IndexField("data", new DataKeyType()));
        try {
            this.index = new BTreeIndex(recordStorageLocation, keyInfo);
            this.index.openIndex();
        } catch(OverlappingFileLockException e) {
            throw new RecordStoreInUseException("Record datastore: " + recordStorageLocation + " is already in use, cannot be opened");
        }
    }

    @Override
    public void closeWriter() throws JasDBStorageException {
        this.index.closeIndex();
    }

    @Override
    public void flush() throws JasDBStorageException {
        index.flushIndex();
    }

    @Override
    public boolean isOpen() {
        return index.getState() != IndexState.CLOSED;
    }

    @Override
    public RecordIterator readAllRecords() throws JasDBStorageException {
        return readAllRecords(-1);
    }

    @Override
    public RecordIterator readAllRecords(int limit) throws JasDBStorageException {
        IndexIterator indexIterator = index.getIndexIterator();

        return new BtreeRecordIteratorImpl(indexIterator, keyInfo.getKeyNameMapper(), limit);
    }

    @Override
    public RecordResult readRecord(UUIDKey documentId) throws JasDBStorageException {
        IndexSearchResultIterator resultIterator = index.searchIndex(new EqualsCondition(documentId), Index.NO_SEARCH_LIMIT);
        if(!resultIterator.isEmpty()) {
            Key key = resultIterator.next();
            DataKey dataKey = (DataKey) key.getKey(keyInfo.getKeyNameMapper(), "data");

            return new BtreeRecordResult(dataKey);
        } else {
            return new BtreeRecordResult();
        }
    }

    @Override
    public void writeRecord(UUIDKey documentId, ClonableDataStream dataStream) throws JasDBStorageException {
        Key documentKey = documentId.cloneKey();
        documentKey.addKey(keyInfo.getKeyNameMapper(), "data", new DataKey(dataStream));

        StatRecord insertDataRecordIndex = StatisticsMonitor.createRecord("insert:data:record");
        index.insertIntoIndex(documentKey);
        insertDataRecordIndex.stop();
    }

    @Override
    public void removeRecord(UUIDKey documentId) throws JasDBStorageException {
        index.removeFromIndex(documentId);
    }

    @Override
    public void updateRecord(UUIDKey documentId, ClonableDataStream dataStream) throws JasDBStorageException {
        Key documentKey = documentId.cloneKey();
        documentKey.addKey(keyInfo.getKeyNameMapper(), "data", new DataKey(dataStream));

        index.updateKey(documentId, documentKey);
    }
}
