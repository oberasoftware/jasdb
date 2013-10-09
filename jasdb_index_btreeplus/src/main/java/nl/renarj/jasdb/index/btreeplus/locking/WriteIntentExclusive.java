package nl.renarj.jasdb.index.btreeplus.locking;

import nl.renarj.jasdb.index.btreeplus.BlockPersister;
import nl.renarj.jasdb.index.btreeplus.IndexBlock;

/**
 * @author: renarj
 * Date: 7-6-12
 * Time: 22:27
 */
public class WriteIntentExclusive implements LockIntent {
    @Override
    public boolean requiresWriteLock(BlockPersister persister, IndexBlock block) {
        return true;
    }
}
