package com.oberasoftware.jasdb.writer.btree;

import com.oberasoftware.jasdb.core.index.query.SimpleIndexField;
import com.oberasoftware.jasdb.core.statistics.StatRecord;
import com.oberasoftware.jasdb.core.statistics.StatisticsMonitor;
import com.oberasoftware.jasdb.api.exceptions.JasDBStorageException;
import com.oberasoftware.jasdb.api.storage.RecordIterator;
import com.oberasoftware.jasdb.api.storage.RecordResult;
import com.oberasoftware.jasdb.api.storage.RecordWriter;
import com.oberasoftware.jasdb.api.storage.ClonableDataStream;
import com.oberasoftware.jasdb.api.index.Index;
import com.oberasoftware.jasdb.api.index.IndexIterator;
import com.oberasoftware.jasdb.api.index.IndexState;
import com.oberasoftware.jasdb.core.index.btreeplus.BTreeIndex;
import com.oberasoftware.jasdb.api.index.keys.Key;
import com.oberasoftware.jasdb.core.index.keys.DataKey;
import com.oberasoftware.jasdb.core.index.keys.UUIDKey;
import com.oberasoftware.jasdb.api.index.keys.KeyInfo;
import com.oberasoftware.jasdb.core.index.keys.keyinfo.KeyInfoImpl;
import com.oberasoftware.jasdb.core.index.keys.types.DataKeyType;
import com.oberasoftware.jasdb.core.index.keys.types.UUIDKeyType;
import com.oberasoftware.jasdb.api.index.query.IndexSearchResultIterator;
import com.oberasoftware.jasdb.core.index.query.EqualsCondition;
import com.oberasoftware.jasdb.api.exceptions.RecordStoreInUseException;

import java.io.File;
import java.nio.channels.OverlappingFileLockException;

/**
 * @author Renze de Vries
 */
public class BtreeIndexRecordWriter implements RecordWriter<UUIDKey> {
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
        this.keyInfo = new KeyInfoImpl(new SimpleIndexField("__ID", new UUIDKeyType()), new SimpleIndexField("data", new DataKeyType()));
        try {
            this.index = new BTreeIndex(recordStorageLocation, keyInfo);
            this.index.openIndex();
        } catch(OverlappingFileLockException e) {
            throw new RecordStoreInUseException("Record datastore: " + recordStorageLocation + " is already in use, cannot be opened");
        }
    }

    @Override
    public void closeWriter() throws JasDBStorageException {
        this.index.close();
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
