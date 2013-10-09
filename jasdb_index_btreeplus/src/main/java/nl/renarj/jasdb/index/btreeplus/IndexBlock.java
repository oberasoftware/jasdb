package nl.renarj.jasdb.index.btreeplus;

import nl.renarj.jasdb.core.exceptions.JasDBStorageException;
import nl.renarj.jasdb.core.storage.datablocks.Block;
import nl.renarj.jasdb.core.storage.datablocks.DataBlock;
import nl.renarj.jasdb.core.utils.ReadWriteLock;
import nl.renarj.jasdb.index.btreeplus.locking.LockIntentType;
import nl.renarj.jasdb.index.btreeplus.persistence.BlockTypes;
import nl.renarj.jasdb.index.keys.Key;

/**
 * @author Renze de Vries
 */
public interface IndexBlock extends Block {
    DataBlock getDataBlock();

    void reset();

    Key getFirst();
    Key getLast();
    int size();

    long memorySize();

    boolean isModified();

    BlockTypes getType();

    long getParentPointer();
    void setParentPointer(long blockPointer);

    LeaveBlock findFirstLeaveBlock(LockIntentType intent) throws JasDBStorageException;
    LeaveBlock findLeaveBlock(LockIntentType intent, Key key) throws JasDBStorageException;

    ReadWriteLock getLockManager();
}
