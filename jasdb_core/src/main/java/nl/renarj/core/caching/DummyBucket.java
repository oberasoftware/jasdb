package nl.renarj.core.caching;

import nl.renarj.core.exceptions.CoreConfigException;

/**
 * Dummy implementation can be used in case caching is disabled.
 * 
 * @author Renze de Vries
 *
 */
public class DummyBucket implements Bucket {
	@Override
	public String getName() {
		return "DummyBucket";
	}

	@Override
	public void configure(CacheConfig config) throws CoreConfigException {
	}

	@Override
	public void put(String key, CachableItem cacheItem) {
	}

	@Override
	public void remove(String key) {
	}

	@Override
	public boolean containsItem(String key) {
		return false;
	}

	@Override
	public CachableItem getItem(String key) {
		return null;
	}

	@Override
	public long getMemSize() {
		return 0;
	}

	@Override
	public int getCachedItems() {
		return 0;
	}

	@Override
	public void closeBucket() {
	}
}
