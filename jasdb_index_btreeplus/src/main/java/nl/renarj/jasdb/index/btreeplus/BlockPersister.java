package nl.renarj.jasdb.index.btreeplus;

import nl.renarj.jasdb.core.caching.MemoryAware;
import nl.renarj.jasdb.core.exceptions.JasDBStorageException;
import nl.renarj.jasdb.index.btreeplus.locking.LockManager;
import nl.renarj.jasdb.index.btreeplus.persistence.BlockTypes;
import nl.renarj.jasdb.index.keys.keyinfo.KeyInfo;

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
