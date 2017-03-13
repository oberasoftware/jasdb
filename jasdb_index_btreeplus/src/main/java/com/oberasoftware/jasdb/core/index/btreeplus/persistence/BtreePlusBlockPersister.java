/*
 * The JASDB software and code is Copyright protected 2012 and owned by Renze de Vries
 * 
 * All the code and design principals in the codebase are also Copyright 2012 
 * protected and owned Renze de Vries. Any unauthorized usage of the code or the 
 * design and principals as in this code is prohibited.
 */
package com.oberasoftware.jasdb.core.index.btreeplus.persistence;

import com.oberasoftware.jasdb.core.statistics.StatRecord;
import com.oberasoftware.jasdb.core.statistics.StatisticsMonitor;
import com.oberasoftware.jasdb.api.caching.CacheRegion;
import com.oberasoftware.jasdb.core.caching.GlobalCachingMemoryManager;
import com.oberasoftware.jasdb.core.caching.LRURegion;
import com.oberasoftware.jasdb.api.exceptions.JasDBStorageException;
import com.oberasoftware.jasdb.api.storage.DataBlock;
import com.oberasoftware.jasdb.api.storage.DataBlockFactory;
import com.oberasoftware.jasdb.core.index.btreeplus.BlockPersister;
import com.oberasoftware.jasdb.core.index.btreeplus.IndexBlock;
import com.oberasoftware.jasdb.core.index.btreeplus.caching.IndexBlockEntry;
import com.oberasoftware.jasdb.core.index.btreeplus.locking.LockManager;
import com.oberasoftware.jasdb.api.index.keys.KeyInfo;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author Renze de Vries
 */
public class BtreePlusBlockPersister implements BlockPersister {
    private static final int BLOCK_TYPE_HEADER_INDEX = 0;

    private final int pageSize;
    private final KeyInfo keyInfo;
    private final DataBlockFactory dataBlockFactory;
    private LockManager lockManager;
    private final int minBlockSize;

    private final Map<BlockTypes, BlockFactory> blockFactories = new HashMap<>(3);

    private Lock lock = new ReentrantLock();

    private CacheRegion<Long, IndexBlockEntry> memoryRegion;

    public BtreePlusBlockPersister(DataBlockFactory dataBlockFactory, int pageSize, KeyInfo keyInfo) {
        this.dataBlockFactory = dataBlockFactory;
        this.lockManager = new LockManager(this);
        this.pageSize = pageSize;
        this.keyInfo = keyInfo;
        this.minBlockSize = (int)Math.floor((double)pageSize / 2);

        initializeBlockFactories();
        memoryRegion = new LRURegion<>(keyInfo.getKeyName());
        GlobalCachingMemoryManager.getGlobalInstance().registerRegion(memoryRegion);
    }

    private void initializeBlockFactories() {
        blockFactories.put(BlockTypes.LEAVEBLOCK, new LeaveBlockFactory(this));
        blockFactories.put(BlockTypes.NODEBLOCK, new NodeBlockFactory(this));
        blockFactories.put(BlockTypes.ROOTBLOCK, new RootBlockFactory(this));
    }

    @Override
    public LockManager getLockManager() {
        return lockManager;
    }

    public DataBlockFactory getDataBlockFactory() {
        return dataBlockFactory;
    }

    @Override
    public long getTotalMemoryUsage() {
        return memoryRegion.memorySize();
    }

    @Override
    public long getCachedBlocks() {
        return memoryRegion.size();
    }

    @Override
    public int getMaxKeys() {
        return pageSize;
    }

    @Override
    public int getMinKeys() {
        return minBlockSize;
    }

    @Override
    public KeyInfo getKeyInfo() {
        return keyInfo;
    }

    @Override
    public IndexBlock loadBlock(long position) throws JasDBStorageException {
        StatRecord loadBlockRecord = StatisticsMonitor.createRecord("btreeplus:persister:loadblock");
        try {
            IndexBlockEntry blockEntry = memoryRegion.getEntry(position);
            if(blockEntry != null) {
                blockEntry.requestToken();
                lockManager.registerBlockUsage(blockEntry.getValue());

                return blockEntry.getValue();
            } else {
                DataBlock dataBlock = dataBlockFactory.loadBlock(position);
                BlockTypes type = BlockTypes.getByTypeDef(dataBlock.getHeader().getInt(BLOCK_TYPE_HEADER_INDEX));
                IndexBlock block = blockFactories.get(type).loadBlock(dataBlock);

                //it could happen the block was loaded concurrently, the cache putEntry will return the correct block entry
                blockEntry = new IndexBlockEntry(this, block);
                blockEntry = memoryRegion.putEntry(position, blockEntry);

                blockEntry.requestToken();
                lockManager.registerBlockUsage(blockEntry.getValue());

                return block;
            }
        } finally {
            loadBlockRecord.stop();
        }
    }

    @Override
    public void persistBlock(IndexBlock block) throws JasDBStorageException {
        if(block.isModified()) {
            blockFactories.get(block.getType()).persistBlock(block);
            block.getDataBlock().flush();
        }
    }

    public void flushAndCloseBlock(IndexBlock block) throws JasDBStorageException {
        persistBlock(block);
        memoryRegion.removeEntry(block.getPosition());
    }

    @Override
    public void markDeleted(IndexBlock block) throws JasDBStorageException {
    }

    @Override
    public void flush() throws JasDBStorageException {
        for(IndexBlockEntry entry : memoryRegion.values()) {
            IndexBlock block = entry.getValue();
            persistBlock(block);
        }
    }

    @Override
    public void close() throws JasDBStorageException {
        flush();
        memoryRegion.clear();
        GlobalCachingMemoryManager.getGlobalInstance().unregisterRegion(keyInfo.getKeyName());
    }

    @Override
    public void releaseBlock(IndexBlock block) {
        memoryRegion.getEntry(block.getPosition()).releaseToken();
    }


    @Override
    public IndexBlock createBlock(BlockTypes blockType, long parentBlock) throws JasDBStorageException {
        StatRecord blockCreateTimer = StatisticsMonitor.createRecord("btreeplus:persister:createblock");
        lock.lock();
        try {
            IndexBlock block = blockFactories.get(blockType).createBlock(parentBlock, dataBlockFactory.getBlockWithSpace(false));
            block.getDataBlock().getHeader().putInt(BLOCK_TYPE_HEADER_INDEX, blockType.getTypeDef());

            IndexBlockEntry entry = new IndexBlockEntry(this, block);
            entry.requestToken();
            memoryRegion.putEntry(block.getPosition(), entry);
            lockManager.registerBlockUsage(block);

            return block;
        } finally {
            lock.unlock();
            blockCreateTimer.stop();
        }
    }

    @Override
    public long getBlockSize(BlockTypes blockType) {
        return -1;
    }
}
