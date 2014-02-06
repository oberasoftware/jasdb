/*
 * The JASDB software and code is Copyright protected 2011 and owned by Renze de Vries
 * 
 * All the code and design principals in the codebase are also Copyright 2011 
 * protected and owned Renze de Vries. Any unauthorized usage of the code or the 
 * design and principals as in this code is prohibited.
 */
package nl.renarj.jasdb.index.btreeplus.caching;

import nl.renarj.core.utilities.collections.OrderedBalancedTree;
import nl.renarj.jasdb.core.exceptions.ConfigurationException;
import nl.renarj.jasdb.index.MemoryManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author Renze de Vries
 */
public abstract class AbstractMemoryManager implements MemoryManager {
    private static final Logger log = LoggerFactory.getLogger(AbstractMemoryManager.class);

    protected final OrderedBalancedTree<Long, AbstractMemoryBlock> idleBlocks;
    protected final Lock lock = new ReentrantLock();
    protected final String index;

    protected AtomicLong totalMemorySize = new AtomicLong();
    protected AtomicLong totalBlocks = new AtomicLong();

    private CachingConfig cachingConfig;

    protected AbstractMemoryManager(String index) {
        this.index = index;
        this.idleBlocks = new OrderedBalancedTree<>();
        this.cachingConfig = CachingConfig.getDefaultCachingConfig();
    }

    public void configure(CachingConfig cachingConfig) throws ConfigurationException {
        this.cachingConfig = cachingConfig;
        log.info("CacheManager loaded for index: {}", index);
        log.info("CacheManager settings for index: {} maxMemSize: {} bytes maxBlocks: {}",
                new Object[] {index, cachingConfig.getMaxMemSize(), cachingConfig.getMaxBlocks()});
    }

    @Override
    public long getMaxMemSize() {
        return cachingConfig.getMaxMemSize();
    }

    @Override
    public long getMaxSize() {
        return cachingConfig.getMaxBlocks();
    }

    @Override
    public long getCachedBlocks() {
        return totalBlocks.get();
    }

    protected void addToIdleList(AbstractMemoryBlock entry) {
        AbstractMemoryBlock lastEntry = idleBlocks.last();
        long entryNr;
        if(lastEntry != null) {
            entryNr = lastEntry.getIdleEntryId() + 1;
        } else {
            entryNr = System.currentTimeMillis();
        }
        entry.setIdleEntryId(entryNr);

        idleBlocks.put(entry.getIdleEntryId(), entry);
    }

    protected boolean checkRecycleRequired() {
        long maxBlocks = cachingConfig.getMaxBlocks();
        long maxMemSize = cachingConfig.getMaxMemSize();

        lock.lock();
        try {
            long totalMemoryUsage = totalMemorySize.get();
            if(maxBlocks != -1 && totalBlocks.get() > maxBlocks) {
                log.debug("Maximum block size has been exceeded: {} on index: {}, reduction is needed", totalBlocks, index);
                return true;
            } else if(maxMemSize != -1 && totalMemoryUsage > maxMemSize) {
                log.debug("Maximum block MemorySize has been exceeded: {} bytes on index: {}, reduction is needed", totalMemoryUsage, index);
                return true;
            } else {
                return false;
            }
        } finally {
            lock.unlock();
        }

    }


}
