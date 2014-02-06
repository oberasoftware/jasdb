package nl.renarj.jasdb.index.btreeplus.locking;

import nl.renarj.jasdb.index.btreeplus.IndexBlock;

/**
* @author Renze de Vries
*/
public class LockEntry {
    private IndexBlock block;
    private LOCK_TYPE type;

    public LockEntry(IndexBlock block, LOCK_TYPE type) {
        this.block = block;
        this.type = type;
    }

    public IndexBlock getBlock() {
        return block;
    }

    public LOCK_TYPE getType() {
        return type;
    }
}
