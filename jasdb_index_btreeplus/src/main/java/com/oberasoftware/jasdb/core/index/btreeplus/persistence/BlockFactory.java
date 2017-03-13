package com.oberasoftware.jasdb.core.index.btreeplus.persistence;

import com.oberasoftware.jasdb.api.exceptions.JasDBStorageException;
import com.oberasoftware.jasdb.api.storage.DataBlock;
import com.oberasoftware.jasdb.core.index.btreeplus.IndexBlock;

/**
 * @author Renze de Vries
 */
public interface BlockFactory<T extends IndexBlock> {
    T loadBlock(DataBlock dataBlock) throws JasDBStorageException;

    void persistBlock(T block) throws JasDBStorageException;

    T createBlock(long parentBlock, DataBlock dataBlock) throws JasDBStorageException;
}
