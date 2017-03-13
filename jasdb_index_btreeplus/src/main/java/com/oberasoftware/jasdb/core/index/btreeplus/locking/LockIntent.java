package com.oberasoftware.jasdb.core.index.btreeplus.locking;

import com.oberasoftware.jasdb.core.index.btreeplus.BlockPersister;
import com.oberasoftware.jasdb.core.index.btreeplus.IndexBlock;

/**
* @author: renarj
* Date: 6-6-12
* Time: 20:30
*/
public interface LockIntent {
    public boolean requiresWriteLock(BlockPersister persister, IndexBlock block);
}
