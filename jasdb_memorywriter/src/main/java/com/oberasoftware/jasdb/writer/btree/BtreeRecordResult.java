package com.oberasoftware.jasdb.writer.btree;

import com.oberasoftware.jasdb.api.exceptions.JasDBStorageException;
import com.oberasoftware.jasdb.api.storage.RecordResult;
import com.oberasoftware.jasdb.api.storage.ClonableDataStream;
import com.oberasoftware.jasdb.core.index.keys.DataKey;

/**
 * @author Renze de Vries
 */
public class BtreeRecordResult implements RecordResult {
    private DataKey dataKey;

    public BtreeRecordResult(DataKey dataKey) {
        this.dataKey = dataKey;
    }

    public BtreeRecordResult() {

    }

    @Override
    public ClonableDataStream getStream() throws JasDBStorageException {
        return dataKey.getStream();
    }

    @Override
    public long getRecordSize() {
        return dataKey.size();
    }

    @Override
    public boolean isRecordFound() {
        return dataKey != null;
    }
}
