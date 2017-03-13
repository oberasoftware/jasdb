package com.oberasoftware.jasdb.core.index.btreeplus.locking;

import com.oberasoftware.jasdb.api.concurrency.ReadWriteLock;
import com.oberasoftware.jasdb.core.index.btreeplus.BlockPersister;
import com.oberasoftware.jasdb.core.index.btreeplus.IndexBlock;

import java.util.LinkedList;

/**
 * @author Renze de Vries
 */
public class LockManager {
    private final ThreadLocal<LockChain> currentLockChain = new ThreadLocal<>();

    private final BlockPersister persister;

    public LockManager(BlockPersister persister) {
        this.persister = persister;
    }

    public void startLockChain() {
        currentLockChain.set(new LockChain());
    }

    public void releaseLockChain() {
        LockChain chain = currentLockChain.get();

        if(chain != null) {
            chain.release();
            currentLockChain.remove();
        }
    }

    public void acquireLock(LockIntentType intent, IndexBlock block) {
        LockChain lockChain = currentLockChain.get();

        lockChain.acquireLock(intent.getIntent(), block);
    }

    public void registerBlockUsage(IndexBlock block) {
        LockChain lockChain = currentLockChain.get();

        if(lockChain != null) {
            lockChain.registerBlockUsage(block);
        }
    }

    private class LockChain {
        private LinkedList<LockEntry> lockEntries;

        private LinkedList<IndexBlock> usedBlocks;

        private LockChain() {
            lockEntries = new LinkedList<>();
            usedBlocks = new LinkedList<>();
        }

        public void registerBlockUsage(IndexBlock block) {
            usedBlocks.add(block);
        }

        public void release() {
            for(LockEntry entry : lockEntries) {
                ReadWriteLock lockManager = entry.getBlock().getLockManager();
                if(entry.getType() == LOCK_TYPE.WRITE) {
                    lockManager.writeUnlock();
                } else {
                    lockManager.readUnlock();
                }
            }

            for(IndexBlock usedBlock : usedBlocks) {
                persister.releaseBlock(usedBlock);
            }
        }

        public void acquireLock(LockIntent intent, IndexBlock block) {
            ReadWriteLock lockManager = block.getLockManager();
            lockManager.readLock();
            boolean shouldWriteLock = intent.requiresWriteLock(persister, block);
            if(shouldWriteLock) {
                lockManager.readUnlock();
                lockManager.writeLock();
                lockEntries.add(new LockEntry(block, LOCK_TYPE.WRITE));
            } else {
                lockEntries.add(new LockEntry(block, LOCK_TYPE.READ));
            }
        }
    }

}
