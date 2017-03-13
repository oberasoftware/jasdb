package com.oberasoftware.jasdb.core.storage;

import com.oberasoftware.jasdb.api.storage.DataBlock;
import com.oberasoftware.jasdb.api.caching.CacheEntry;
import com.oberasoftware.jasdb.api.exceptions.JasDBStorageException;

/**
 * @author Renze de Vries
 */
public class BlockEntry<T extends DataBlock> implements CacheEntry<T> {
    private int openBlockCount = 0;
    private T dataBlock;

    public BlockEntry(T dataBlock) {
        this.dataBlock = dataBlock;
    }

    public long getPosition() {
        return dataBlock.getPosition();
    }

    public void incrementBlockCount() {
        this.openBlockCount++;
    }

    public void decrementBlockCount() {
        this.openBlockCount--;
    }

    @Override
    public boolean isInUse() {
        return openBlockCount != 0;
    }

    @Override
    public long memorySize() {
        return dataBlock.size();
    }

    @Override
    public T getValue() {
        return dataBlock;
    }

    @Override
    public void release() throws JasDBStorageException {
        dataBlock.close();
    }

    @Override
    public String toString() {
        return "BlockEntry{" +
                "openBlockCount=" + openBlockCount +
                ", dataBlock=" + dataBlock +
                '}';
    }
}
