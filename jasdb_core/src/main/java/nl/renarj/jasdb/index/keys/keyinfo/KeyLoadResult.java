package nl.renarj.jasdb.index.keys.keyinfo;

import nl.renarj.jasdb.core.storage.datablocks.DataBlock;
import nl.renarj.jasdb.index.keys.Key;

/**
 * @author Renze de Vries
 */
public class KeyLoadResult {
    private Key loadedKey;
    private DataBlock endBlock;
    private int nextOffset;

    public KeyLoadResult(Key loadedKey, DataBlock endBlock, int nextOffset) {
        this.loadedKey = loadedKey;
        this.endBlock = endBlock;
        this.nextOffset = nextOffset;
    }

    public Key getLoadedKey() {
        return loadedKey;
    }

    public DataBlock getEndBlock() {
        return endBlock;
    }

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
