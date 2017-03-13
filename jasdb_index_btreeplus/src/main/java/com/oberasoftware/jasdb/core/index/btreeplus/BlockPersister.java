package com.oberasoftware.jasdb.core.index.btreeplus;

import com.oberasoftware.jasdb.api.caching.MemoryAware;
import com.oberasoftware.jasdb.api.exceptions.JasDBStorageException;
import com.oberasoftware.jasdb.core.index.btreeplus.locking.LockManager;
import com.oberasoftware.jasdb.core.index.btreeplus.persistence.BlockTypes;
import com.oberasoftware.jasdb.api.index.keys.KeyInfo;

/**
 * @author Renze de Vries
 * Date: 5/23/12
 * Time: 11:22 PM
 */
public interface BlockPersister extends MemoryAware {
    int getMaxKeys();
    int getMinKeys();
    KeyInfo getKeyInfo();

    IndexBlock loadBlock(long position) throws JasDBStorageException;

    void persistBlock(IndexBlock block) throws JasDBStorageException;

    void markDeleted(IndexBlock block) throws JasDBStorageException;

    void flush() throws JasDBStorageException;

    void close() throws JasDBStorageException;

    void releaseBlock(IndexBlock block);

    public IndexBlock createBlock(BlockTypes blockType, long parentBlock) throws JasDBStorageException;

    long getBlockSize(BlockTypes blockType);

    LockManager getLockManager();
}
