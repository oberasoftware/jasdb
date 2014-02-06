package nl.renarj.jasdb.index.btreeplus.persistence;

import nl.renarj.jasdb.core.exceptions.JasDBStorageException;
import nl.renarj.jasdb.core.storage.datablocks.DataBlock;
import nl.renarj.jasdb.index.btreeplus.IndexBlock;

/**
 * @author Renze de Vries
 */
public interface BlockFactory<T extends IndexBlock> {
    T loadBlock(DataBlock dataBlock) throws JasDBStorageException;

    void persistBlock(T block) throws JasDBStorageException;

    T createBlock(long parentBlock, DataBlock dataBlock) throws JasDBStorageException;
}
