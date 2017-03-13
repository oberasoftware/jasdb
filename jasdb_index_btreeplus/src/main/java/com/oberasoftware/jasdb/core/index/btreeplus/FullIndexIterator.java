package com.oberasoftware.jasdb.core.index.btreeplus;

import com.oberasoftware.jasdb.api.exceptions.JasDBStorageException;
import com.oberasoftware.jasdb.api.index.IndexIterator;
import com.oberasoftware.jasdb.core.index.btreeplus.locking.LockIntentType;
import com.oberasoftware.jasdb.core.index.btreeplus.locking.LockManager;
import com.oberasoftware.jasdb.api.index.keys.Key;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;

/**
 * @author Renze de Vries
 */
public class FullIndexIterator implements IndexIterator {
    private static final Logger LOG = LoggerFactory.getLogger(FullIndexIterator.class);

    private RootBlock rootBlock;
    private BlockPersister persister;
    private LockManager lockManager;

    private LeaveBlock currentBlock;
    private List<Key> blockKeys;
    private int blockIndex;

    public FullIndexIterator(RootBlock rootBlock, LockManager lockManager, BlockPersister persister) {
        this.persister = persister;
        this.rootBlock = rootBlock;
        this.lockManager = lockManager;
    }

    @Override
    public void close() {
        currentBlock = null;
        blockKeys = null;

        lockManager.releaseLockChain();
    }

    @Override
    public void reset() {
        close();
    }

    @Override
    public Iterator<Key> iterator() {
        return this;
    }

    @Override
    public boolean hasNext() {
        try {
            ensureBlockLoaded();

            return blockKeys != null && blockIndex < blockKeys.size();
        } catch(JasDBStorageException e) {
            LOG.error("Unable to load next block", e);
        }

        close();
        return false;
    }

    private void ensureBlockLoaded() throws JasDBStorageException {
        LeaveBlock nextBlock = null;
        if(blockKeys != null && blockIndex == blockKeys.size()) {
            currentBlock.getLockManager().readLock();
            try {
                long nextBlockPointer = currentBlock.getProperties().getNextBlock();
                if(nextBlockPointer > 0) {
                    nextBlock = (LeaveBlock) persister.loadBlock(nextBlockPointer);
                }
            } finally {
                currentBlock.getLockManager().readUnlock();
            }
        } else if(currentBlock == null) {
            lockManager.startLockChain();
            rootBlock.getLockManager().readLock();
            try {
                nextBlock = rootBlock.findFirstLeaveBlock(LockIntentType.READ);
            } finally {
                rootBlock.getLockManager().readUnlock();
            }
        }

        if(nextBlock != null) {
            nextBlock.getLockManager().readLock();
            try {
                currentBlock = nextBlock;
                blockKeys = nextBlock.getValues();
                blockIndex = 0;
            } finally {
                nextBlock.getLockManager().readUnlock();
            }
        }

    }

    @Override
    public Key next() {
        if(hasNext() && blockKeys != null) {
            Key loadedKey = blockKeys.get(blockIndex);
            blockIndex++;

            return loadedKey;
        } else {
            return null;
        }
    }

    @Override
    public void remove() {

    }
}
