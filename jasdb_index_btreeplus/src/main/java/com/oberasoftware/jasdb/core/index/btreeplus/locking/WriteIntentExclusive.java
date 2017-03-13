package com.oberasoftware.jasdb.core.index.btreeplus.locking;

import com.oberasoftware.jasdb.core.index.btreeplus.BlockPersister;
import com.oberasoftware.jasdb.core.index.btreeplus.IndexBlock;

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
