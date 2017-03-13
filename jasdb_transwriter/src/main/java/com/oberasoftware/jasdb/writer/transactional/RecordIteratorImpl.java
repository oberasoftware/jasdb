package com.oberasoftware.jasdb.writer.transactional;

import com.oberasoftware.jasdb.api.exceptions.JasDBStorageException;
import com.oberasoftware.jasdb.api.storage.RecordIterator;
import com.oberasoftware.jasdb.api.storage.RecordResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Optional;

/**
 * @author Renze de Vries
 */
public class RecordIteratorImpl implements RecordIterator {
    private static final Logger LOG = LoggerFactory.getLogger(RecordIteratorImpl.class);

    private Writer recordWriter;

    private long nextRecordPointer;
    private long startRecordPointer;

    private int limit;
    private long foundRecords = 0;

    private RecordResultImpl nextLoadedRecord;

    protected RecordIteratorImpl(Writer recordWriter, long startRecordPointer, int limit) {
        this.recordWriter = recordWriter;
        this.nextRecordPointer = startRecordPointer;
        this.limit = limit;
        this.startRecordPointer = startRecordPointer;
    }

    @Override
    public void close() {
        foundRecords = 0;
        nextRecordPointer = startRecordPointer;
        nextLoadedRecord = null;
    }

    @Override
    public Iterator<RecordResult> iterator() {
        return this;
    }

    @Override
    public boolean hasNext() {
        if(nextLoadedRecord == null) {
            loadNextRecord();
        }

        return nextLoadedRecord != null && (foundRecords <= limit || limit == -1);
    }

    private void loadNextRecord() {
        try {
            RecordResultImpl foundRecord;
            do {
                foundRecord = recordWriter.readRecord(() -> Optional.of(nextRecordPointer));
                nextRecordPointer = nextRecordPointer + foundRecord.getRecordSize();
            } while(foundRecord.getRecordFlag() != RECORD_FLAG.ACTIVE && foundRecord.getRecordFlag() != RECORD_FLAG.EMPTY);

            if(foundRecord.getRecordFlag() == RECORD_FLAG.ACTIVE) {
                foundRecords++;
                nextLoadedRecord = foundRecord;
            } else {
                nextLoadedRecord = null;
            }
        } catch(JasDBStorageException e) {
            LOG.error("Unable to find next record, error accessing storage", e);
        }

    }

    @Override
    public RecordResultImpl next() {
        if(nextLoadedRecord == null) {
            loadNextRecord();
        }

        RecordResultImpl loadedRecord = nextLoadedRecord;
        nextLoadedRecord = null;
        return loadedRecord;
    }

    @Override
    public void reset() {
        this.nextRecordPointer = startRecordPointer;
        this.foundRecords = 0;
    }

    @Override
    public void remove() {
		/* not implemented */
    }

}
