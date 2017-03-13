package com.oberasoftware.jasdb.core.index.btreeplus.locking;

import com.oberasoftware.jasdb.core.index.btreeplus.BlockPersister;
import com.oberasoftware.jasdb.core.index.btreeplus.IndexBlock;
import com.oberasoftware.jasdb.core.index.btreeplus.LeaveBlock;
import com.oberasoftware.jasdb.core.index.btreeplus.RootBlock;

/**
 * @author Renze de Vries
 * Date: 6-6-12
 * Time: 21:43
 */
public class UpdateIntent implements LockIntent {
    @Override
    public boolean requiresWriteLock(BlockPersister persister, IndexBlock block) {
        if(block instanceof RootBlock) {
            return ((RootBlock)block).isLeave();
        } else {
            return block instanceof LeaveBlock;
        }
    }
}
