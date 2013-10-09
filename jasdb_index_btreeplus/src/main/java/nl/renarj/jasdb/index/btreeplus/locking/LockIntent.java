package nl.renarj.jasdb.index.btreeplus.locking;

import nl.renarj.jasdb.index.btreeplus.BlockPersister;
import nl.renarj.jasdb.index.btreeplus.IndexBlock;

/**
* @author: renarj
* Date: 6-6-12
* Time: 20:30
*/
public interface LockIntent {
    public boolean requiresWriteLock(BlockPersister persister, IndexBlock block);
}
