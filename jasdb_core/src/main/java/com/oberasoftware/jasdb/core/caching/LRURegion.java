package com.oberasoftware.jasdb.core.caching;

import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.oberasoftware.jasdb.api.caching.CacheEntry;
import com.oberasoftware.jasdb.api.caching.CacheRegion;
import com.oberasoftware.jasdb.core.collections.OrderedBalancedTree;
import com.oberasoftware.jasdb.api.exceptions.RuntimeJasDBException;
import com.oberasoftware.jasdb.api.exceptions.JasDBStorageException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author Renze de Vries
 */
public class LRURegion<T extends CacheEntry> implements CacheRegion<Long, T> {
    private static final Logger LOG = LoggerFactory.getLogger(LRURegion.class);

    protected final OrderedBalancedTree<Long, EntryWrapper> cachedBlocks;
    protected final OrderedBalancedTree<Long, Long> blockAccessTime;

    private long entryCounter = 0;
    private long lastAccess = System.currentTimeMillis();

    private Lock lock = new ReentrantLock();

    private String name;

    public LRURegion(String name) {
        this.cachedBlocks = new OrderedBalancedTree<>();
        this.blockAccessTime = new OrderedBalancedTree<>();
        this.name = name;
    }

    @Override
    public long lastRegionAccess() {
        return lastAccess;
    }

    @Override
    public String name() {
        return this.name;
    }

    @Override
    public long memorySize() {
        lock.lock();
        try {
            long memorySize = 0;
            for(EntryWrapper entryWrapper : cachedBlocks.values()) {
                memorySize += entryWrapper.getEntry().memorySize();
            }
            return memorySize;
        } finally {
            lock.unlock();
        }
    }

    private void updateAccess() {
        lastAccess = System.currentTimeMillis();
    }

    @Override
    public long reduceBy(long reduceSize) {
        long initialMemory = memorySize();
        LOG.debug("initial memory: {}", initialMemory);
        long targetMemory = initialMemory - reduceSize;
        targetMemory = targetMemory > 0 ? targetMemory : 0; //should never be negative
        LOG.debug("Target memory: {}", targetMemory);
        Set<Long> checkedKeys = new HashSet<>();
        while(memorySize() > targetMemory) {
            Long key = blockAccessTime.first();

            if(!checkedKeys.contains(key)) {
                LOG.debug("Removing key: {}", key);
                removeEntry(key);
                checkedKeys.add(key);
            } else {
                LOG.debug("Cannot reduce memory footprint of region: {} further, key: {} cannot be found", this, key);
                break;
            }
        }
        long afterSize = memorySize();
        LOG.debug("After size: {}", afterSize);
        return initialMemory - memorySize();
    }

    @Override
    public T putEntry(Long key, T entry) {
        lock.lock();
        try {
            EntryWrapper wrapper = cachedBlocks.get(key);
            if(wrapper == null) {
                cachedBlocks.put(key, new EntryWrapper(entry, entryCounter));
                blockAccessTime.put(entryCounter, key);
                entryCounter++;
                return entry;
            } else {
                return wrapper.getEntry();
            }
        } finally {
            updateAccess();
            lock.unlock();
        }
    }

    @Override
    public boolean contains(Long key) {
        return cachedBlocks.contains(key);
    }

    @Override
    public T getEntry(Long key) {
        lock.lock();
        try {
            EntryWrapper entryWrapper = cachedBlocks.get(key);
            if(entryWrapper != null) {
                updateEntryId(key, entryWrapper);
                return entryWrapper.getEntry();
            } else {
                return null;
            }
        } finally {
            updateAccess();
            lock.unlock();
        }
    }

    @Override
    public boolean removeEntry(Long key) {
        lock.lock();
        try {
            EntryWrapper entryWrapper = cachedBlocks.get(key);
            if(!entryWrapper.getEntry().isInUse()) {
                LOG.debug("Removing entry: {}", entryWrapper);
                try {
                    entryWrapper.getEntry().release();
                } catch(JasDBStorageException e) {
                    throw new RuntimeJasDBException("Unable to cleanly close index memory block", e);
                }

                cachedBlocks.remove(key);
                blockAccessTime.remove(entryWrapper.getEntryId());
                updateAccess();
                return true;
            } else {
                updateEntryId(key, entryWrapper);
                return false;
            }
        } finally {
            lock.unlock();
        }
    }

    private void updateEntryId(Long key, EntryWrapper entryWrapper) {
        blockAccessTime.remove(entryWrapper.getEntryId());
        entryWrapper.setEntryId(entryCounter);
        blockAccessTime.put(entryWrapper.getEntryId(), key);
        entryCounter++;
    }

    @Override
    public Collection<T> values() {
        lock.lock();
        try {
            return Collections2.transform(cachedBlocks.values(), new Function<EntryWrapper, T>() {
                @Override
                public T apply(EntryWrapper entryWrapper) {
                    return entryWrapper.getEntry();
                }
            });
        } finally {
            lock.unlock();
        }
    }

    @Override
    public int size() {
        return cachedBlocks.size();
    }

    @Override
    public void clear() {
        lock.lock();
        try {
            cachedBlocks.reset();
            blockAccessTime.reset();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public String toString() {
        return "LRURegion{" +
                "lastAccess=" + lastAccess +
                ",memory=" + memorySize() + "}";
    }

    private class EntryWrapper {
        private T entry;
        private Long entryId;

        private EntryWrapper(T entry, Long entryId) {
            this.entry = entry;
            this.entryId = entryId;
        }

        public T getEntry() {
            return entry;
        }

        public Long getEntryId() {
            return entryId;
        }

        public void setEntryId(Long entryId) {
            this.entryId = entryId;
        }

        @Override
        public String toString() {
            return "EntryWrapper{" +
                    "entry=" + entry +
                    ", entryId=" + entryId +
                    '}';
        }
    }
}
