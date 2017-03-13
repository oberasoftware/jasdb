package com.oberasoftware.jasdb.core.index.btreeplus;

import com.oberasoftware.jasdb.api.exceptions.JasDBStorageException;
import com.oberasoftware.jasdb.api.storage.Block;
import com.oberasoftware.jasdb.api.storage.DataBlock;
import com.oberasoftware.jasdb.api.concurrency.ReadWriteLock;
import com.oberasoftware.jasdb.core.index.btreeplus.locking.LockIntentType;
import com.oberasoftware.jasdb.core.index.btreeplus.persistence.BlockTypes;
import com.oberasoftware.jasdb.api.index.keys.Key;

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
