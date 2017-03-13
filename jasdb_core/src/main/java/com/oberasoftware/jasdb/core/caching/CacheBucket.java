package com.oberasoftware.jasdb.core.caching;

import com.oberasoftware.jasdb.api.caching.Bucket;
import com.oberasoftware.jasdb.api.model.CacheConfig;
import com.oberasoftware.jasdb.api.caching.CachableItem;
import com.oberasoftware.jasdb.api.exceptions.CoreConfigException;
import com.oberasoftware.jasdb.core.statistics.StatRecord;
import com.oberasoftware.jasdb.core.statistics.StatisticsMonitor;
import com.oberasoftware.jasdb.core.collections.OrderedBalancedTree;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public final class CacheBucket implements Bucket {
	private final Logger log = LoggerFactory.getLogger(CacheBucket.class);
	
	private final OrderedBalancedTree<String, CacheElement> cachedItems;
    private final OrderedBalancedTree<Long, String> lruItems;
	
	private AtomicLong totalMemorySize = new AtomicLong(0);
	
	private final Lock lock = new ReentrantLock();

	private CacheConfig cacheConfig;
	
	private String bucketName;
	
	protected CacheBucket(String bucketName) {
		this.bucketName = bucketName;
		cachedItems = new OrderedBalancedTree<>();
        lruItems = new OrderedBalancedTree<>();
		this.cacheConfig = new CacheConfig(false, -1, -1);
	}
	
	@Override
	public void configure(CacheConfig config) throws CoreConfigException {
		this.cacheConfig = config;

		log.info("CacheBucket: {} settings maxMemSize: {} bytes, maxItems: {}",
				bucketName, config.getMaxMemSize(), config.getMaxSize());
		
	}

	@Override
	public String getName() {
		return this.bucketName;
	}

	public void closeBucket() {
		lock.lock();
		try {
			cachedItems.reset();
			totalMemorySize.set(0);
		} finally {
			lock.unlock();
		}
	}
	
	/* (non-Javadoc)
	 * @see com.oberasoftware.jasdb.api.caching.Bucket#put(java.lang.String, com.oberasoftware.jasdb.api.caching.CachableItem)
	 */
	@Override
	public void put(String key, CachableItem cacheItem) {
		lock.lock();
		try {
            CacheElement element = new CacheElement(key, cacheItem);
            cachedItems.put(key, element);

            Long entryId = getEntryId();
            element.setEntryId(entryId);
            lruItems.put(entryId, key);

			totalMemorySize.addAndGet(cacheItem.getObjectSize());
			
			runInvalidator();
		} finally {
			lock.unlock();
		}
	}
	
	/* (non-Javadoc)
	 * @see com.oberasoftware.jasdb.api.caching.Bucket#remove(java.lang.String)
	 */
	@Override
	public void remove(String key) {
		lock.lock();
		try {
			log.debug("Removing item from cache: {}", key);
            CacheElement element = cachedItems.get(key);
            if(element != null) {
                Long entryId = element.getEntryId();
                cachedItems.remove(key);
                lruItems.remove(entryId);

                long current = totalMemorySize.get();
                totalMemorySize.set(current - element.getCachedItem().getObjectSize());
            }
		} finally {
			lock.unlock();
		}
	}

    private Long getEntryId() {
        Long entryId = lruItems.lastKey();
        if(entryId != null) {
            entryId++;
        } else {
            entryId = System.currentTimeMillis();
        }

        return entryId;
    }
	
	public void updateAccess(String key) {
		lock.lock();
		try {
			log.trace("Cache hit for key: {}", key);
            CacheElement cacheElement = cachedItems.get(key);
            if(cacheElement != null) {
                lruItems.remove(cacheElement.getEntryId());

                long entryId = getEntryId();
                cacheElement.setEntryId(entryId);
                lruItems.put(entryId, key);

                runInvalidator();
            }
		} finally {
			lock.unlock();
		}
	}
	
	/* (non-Javadoc)
	 * @see com.oberasoftware.jasdb.api.caching.Bucket#containsItem(java.lang.String)
	 */
	@Override
	public boolean containsItem(String key) {
		lock.lock();
		try {
			return cachedItems.contains(key);
		} finally {
			lock.unlock();
		}
	}
	
	/* (non-Javadoc)
	 * @see com.oberasoftware.jasdb.api.caching.Bucket#getItem(java.lang.String)
	 */
	@Override
	public CachableItem getItem(String key) {
		lock.lock();
		try {
			updateAccess(key);

			CacheElement element = cachedItems.get(key); //new CacheElement(key, null));
			if(element != null) {
				return element.getCachedItem();
			} else {
				return null;
			}
		} finally {
			lock.unlock();
		}
	}
	
	/* (non-Javadoc)
	 * @see com.oberasoftware.jasdb.api.caching.Bucket#getMemSize()
	 */
	@Override
	public long getMemSize() {
		return this.totalMemorySize.get();
	}
	
	/* (non-Javadoc)
	 * @see com.oberasoftware.jasdb.api.caching.Bucket#getCachedItems()
	 */
	@Override
	public int getCachedItems() {
		lock.lock(); 
		try {
			return cachedItems.size();
		} finally {
			lock.unlock();
		}
	}
	
	private boolean checkOverflow() {
		lock.lock();
		try {
			int itemSize = cachedItems.size();
			long totalMemoryUsage = totalMemorySize.get();
			if(cacheConfig.getMaxSize() != -1 && itemSize > cacheConfig.getMaxSize()) {
				log.debug("Bucket overflow: Maximum bucket size has been exceeded: {} on bucket: {}", itemSize, bucketName);
				return true;
			} else if(cacheConfig.getMaxMemSize() != -1 && totalMemoryUsage > cacheConfig.getMaxMemSize()) {
				log.debug("Bucket overflow: Maximum bucket MemorySize has been exceeded: {} bytes on bucket: {}", totalMemoryUsage, bucketName);
				return true;
			} else {
				return false;
			}
		} finally {
			lock.unlock();
		}
	}
	
	public void runInvalidator() {
		if(checkOverflow()) {
			StatRecord cacheOverflowRemove = StatisticsMonitor.createRecord("cache:overflow:remove");
			lock.lock();
			
			try {
				CacheElement cachedElement = cachedItems.get(lruItems.first());
                if(cachedElement != null) {
                    log.debug("Removing last item from the list: {}", cachedElement.getKey());
                    remove(cachedElement.getKey());
                }
			} finally {
				lock.unlock();
				cacheOverflowRemove.stop();
			}
		} 
	}
}
