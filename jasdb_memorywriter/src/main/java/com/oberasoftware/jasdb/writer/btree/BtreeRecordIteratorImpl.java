/*
 * The JASDB software and code is Copyright protected 2011 and owned by Renze de Vries
 * 
 * All the code and design principals in the codebase are also Copyright 2011 
 * protected and owned Renze de Vries. Any unauthorized usage of the code or the 
 * design and principals as in this code is prohibited.
 */
package com.oberasoftware.jasdb.writer.btree;

import com.oberasoftware.jasdb.api.storage.RecordIterator;
import com.oberasoftware.jasdb.api.storage.RecordResult;
import com.oberasoftware.jasdb.api.index.IndexIterator;
import com.oberasoftware.jasdb.api.index.keys.Key;
import com.oberasoftware.jasdb.core.index.keys.DataKey;
import com.oberasoftware.jasdb.api.index.keys.KeyNameMapper;

import java.util.Iterator;

public class BtreeRecordIteratorImpl implements RecordIterator {
	private int limit;
	private long foundRecords = 0;

    private IndexIterator indexIterator;

    private RecordResult nextLoadedRecord;
    private KeyNameMapper keyNameMapper;

	public BtreeRecordIteratorImpl(IndexIterator indexIterator, KeyNameMapper keyNameMapper, int limit) {
		this.limit = limit;
        this.indexIterator = indexIterator;
        this.keyNameMapper = keyNameMapper;
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
        if(indexIterator.hasNext()) {
            Key key = indexIterator.next();
            DataKey dataKey = (DataKey) key.getKey(keyNameMapper, "data");
            nextLoadedRecord = new BtreeRecordResult(dataKey);

            foundRecords++;
        } else {
            nextLoadedRecord = null;
        }
    }

	@Override
	public RecordResult next() {
        if(nextLoadedRecord == null) {
            loadNextRecord();
        }

        RecordResult loadedRecord = nextLoadedRecord;
        nextLoadedRecord = null;
        return loadedRecord;
    }

	/* (non-Javadoc)
	 * @see nl.renarj.pojodb.storage.recordstore.RecordIterator#reset()
	 */
	@Override
	public void reset() {
        close();
	}

    @Override
    public void close() {
        this.indexIterator.reset();
        this.foundRecords = 0;
    }

    @Override
	public void remove() {
		/* not implemented */
	}
}
