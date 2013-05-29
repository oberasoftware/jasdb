package nl.renarj.jasdb.core.storage.datablocks.impl;

import nl.renarj.jasdb.core.caching.CacheEntry;
import nl.renarj.jasdb.core.storage.datablocks.DataBlock;

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
}
