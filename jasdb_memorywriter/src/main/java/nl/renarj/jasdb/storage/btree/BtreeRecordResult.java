package nl.renarj.jasdb.storage.btree;

import nl.renarj.jasdb.core.exceptions.JasDBStorageException;
import nl.renarj.jasdb.core.storage.RecordResult;
import nl.renarj.jasdb.core.streams.ClonableDataStream;
import nl.renarj.jasdb.index.keys.impl.DataKey;

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
