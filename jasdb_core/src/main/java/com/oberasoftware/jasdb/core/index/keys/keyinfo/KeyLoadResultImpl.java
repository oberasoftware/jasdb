package com.oberasoftware.jasdb.core.index.keys.keyinfo;

import com.oberasoftware.jasdb.api.index.keys.KeyLoadResult;
import com.oberasoftware.jasdb.api.storage.DataBlock;
import com.oberasoftware.jasdb.api.index.keys.Key;

/**
 * @author Renze de Vries
 */
public class KeyLoadResultImpl implements KeyLoadResult {
    private Key loadedKey;
    private DataBlock endBlock;
    private int nextOffset;

    public KeyLoadResultImpl(Key loadedKey, DataBlock endBlock, int nextOffset) {
        this.loadedKey = loadedKey;
        this.endBlock = endBlock;
        this.nextOffset = nextOffset;
    }

    @Override
    public Key getLoadedKey() {
        return loadedKey;
    }

    @Override
    public DataBlock getEndBlock() {
        return endBlock;
    }

    @Override
    public int getNextOffset() {
        return nextOffset;
    }

    @Override
    public String toString() {
        return "KeyLoadResult{" +
                "loadedKey=" + loadedKey +
                ", endBlock=" + endBlock +
                ", nextOffset=" + nextOffset +
                '}';
    }
}
