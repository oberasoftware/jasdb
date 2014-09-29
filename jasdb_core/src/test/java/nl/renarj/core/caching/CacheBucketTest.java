package nl.renarj.core.caching;

import junit.framework.Assert;
import nl.renarj.core.exceptions.CoreConfigException;
import nl.renarj.core.utilities.configuration.ConfigurationProperty;
import nl.renarj.core.utilities.configuration.ManualConfiguration;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class CacheBucketTest {
	private static final Logger LOG = LoggerFactory.getLogger(CacheBucketTest.class);
	
	@Test
	public void testItemsGetCached() throws Exception {
		int cachedItems = 1000;
		int itemMemSize = 100;
		
		Bucket bucket = new CacheBucket("testBucket");
		bucket.configure(new CacheConfig(true, -1, -1));
		for(int i=0; i<cachedItems; i++) {
			bucket.put("cacheItem" + i, new CacheItemWrapper(new String("Cached item: " + i), itemMemSize));
		}
		
		long expectedMemSize = cachedItems * itemMemSize;
		Assert.assertEquals("Unexpected memory size", expectedMemSize, bucket.getMemSize());
		Assert.assertEquals("Unexpected amount of items cached", cachedItems, bucket.getCachedItems());
		
		for(int i=0; i<cachedItems; i++) {
			String cacheKey = "cacheItem" + i;
			CachableItem element = bucket.getItem(cacheKey);
			CacheItemWrapper itemWrapper = (CacheItemWrapper) element;
			Assert.assertTrue(itemWrapper.getValue() instanceof String);
			Assert.assertEquals("Unexpected cached item value", "Cached item: " + i, itemWrapper.getValue());
		}
	}
	
	@Test
	public void testItemsCacheLimitedItemSize() throws Exception {
		int cachedItems = 100000;
		int maxAllowedCached = 1000;
		int itemMemSize = 100;
		
		Bucket bucket = new CacheBucket("testBucket");
		bucket.configure(new CacheConfig(true, -1, maxAllowedCached));
		
		for(int i=0; i<cachedItems; i++) {
			bucket.put("cacheItem" + i, new CacheItemWrapper(new String("Cached item: " + i), itemMemSize));
		}

		long expectedMemSize = maxAllowedCached * itemMemSize;
		Assert.assertEquals("Unexpected amount of items cached", maxAllowedCached, bucket.getCachedItems());
		Assert.assertEquals("Unexpected memory size", expectedMemSize, bucket.getMemSize());
	}

    @Test
    public void testLruPolicy() throws Exception {
        int maxItems = 10;
        Bucket bucket = new CacheBucket("testBucket");
        bucket.configure(new CacheConfig(true, -1, maxItems));

        for(int i=0; i<maxItems + 1; i++) {
            bucket.put("cacheItem" + i, new CacheItemWrapper(new String("Cached item: " + i), 100));
        }

        assertNull(bucket.getItem("cacheItem" + 0));
        assertNotNull(bucket.getItem("cacheItem" + maxItems));
    }
	
	@Test
	public void testConcurrentReadWrite() throws Exception {
		int cacheMaxSize = 100000;
		int nrThreads = 10;
		int maxAllowedCached = 1000;
		
		Map<String, String> configurationOptions = new HashMap<String, String>();
		configurationOptions.put("Enabled", "true");
		configurationOptions.put("CheckInterval", "2s");
		ManualConfiguration manualConfig = new ManualConfiguration("Caching", configurationOptions);
		manualConfig.addChildConfiguration("./Property[@Name='MaxItems']", new ConfigurationProperty("MaxItems", String.valueOf(maxAllowedCached)));
		
		CacheManager cacheManager = new CacheManager();
		cacheManager.configure(manualConfig);
		cacheManager.startCacheManager();
		
		Map<Thread, ReadWriteThread> readWriteThreds = new HashMap<Thread, ReadWriteThread>();
		for(int i=0; i<nrThreads; i++) {
			ReadWriteThread rwThread = new ReadWriteThread(cacheManager, cacheMaxSize);
			Thread thread = new Thread(rwThread);
			readWriteThreds.put(thread, rwThread);
			thread.start();
		}
		
		int totalMisses = 0;
		int totalHits = 0;
		for(Map.Entry<Thread, ReadWriteThread> threadEntry : readWriteThreds.entrySet()) {
			threadEntry.getKey().join();
			
			LOG.info("ReadWriteThread has finished with: {} misses and {} hits", 
					threadEntry.getValue().getMisses(), threadEntry.getValue().getHits());
			
			totalMisses += threadEntry.getValue().getMisses();
			totalHits += threadEntry.getValue().getHits();
		}

		LOG.debug("Total misses: {}", totalMisses);
		LOG.debug("Total hits: {}", totalHits);
		Assert.assertTrue("Misses should be smaller than nrthreads * max", (nrThreads * cacheMaxSize) > totalMisses);		
	}
	
	private class ReadWriteThread implements Runnable {
		private Bucket bucket;
		private int cacheSize = 10000;
		
		private int cacheHits = 0;
		private int cacheMisses = 0;
		
		private ReadWriteThread(CacheManager manager, int cacheSize) throws CoreConfigException {
			this.bucket = manager.getBucket("testbucket");
			this.cacheSize = cacheSize;
		}
		
		public int getHits() {
			return this.cacheHits;
		}
		
		public int getMisses() {
			return this.cacheMisses;
		}
		
		public void run() {
			for(int i=0; i<cacheSize; i++) {
				String key = "Key" + i + Thread.currentThread().getId();
				String value = "My Value" + i;
				bucket.put(key, new CacheItemWrapper(value, value.getBytes().length));
				
				if(bucket.getItem(key) != null) {
					cacheHits++;
				} else {
					cacheMisses++;
				}
			}
		}
	}
}
