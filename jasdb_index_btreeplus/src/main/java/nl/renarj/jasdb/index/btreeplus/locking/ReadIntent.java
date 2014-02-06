package nl.renarj.jasdb.index.btreeplus.locking;

import nl.renarj.jasdb.index.btreeplus.BlockPersister;
import nl.renarj.jasdb.index.btreeplus.IndexBlock;

/**
 * @author Renze de Vries
 * Date: 6-6-12
 * Time: 21:36
 */
public class ReadIntent implements LockIntent {
    @Override
    public boolean requiresWriteLock(BlockPersister persister, IndexBlock block) {
        return false;
    }
}
