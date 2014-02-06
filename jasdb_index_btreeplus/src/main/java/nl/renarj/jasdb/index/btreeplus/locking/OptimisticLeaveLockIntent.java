package nl.renarj.jasdb.index.btreeplus.locking;

import nl.renarj.jasdb.index.btreeplus.BlockPersister;
import nl.renarj.jasdb.index.btreeplus.IndexBlock;
import nl.renarj.jasdb.index.btreeplus.LeaveBlock;
import nl.renarj.jasdb.index.btreeplus.RootBlock;

/**
 * @author Renze de Vries
 */
public class OptimisticLeaveLockIntent implements LockIntent {
    @Override
    public boolean requiresWriteLock(BlockPersister persister, IndexBlock block) {
        if(block instanceof RootBlock) {
            return ((RootBlock)block).isLeave();
        } else {
            return block instanceof LeaveBlock;
        }
    }
}
