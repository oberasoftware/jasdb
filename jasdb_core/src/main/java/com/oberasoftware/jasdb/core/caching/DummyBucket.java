package com.oberasoftware.jasdb.core.caching;

import com.oberasoftware.jasdb.api.caching.Bucket;
import com.oberasoftware.jasdb.api.model.CacheConfig;
import com.oberasoftware.jasdb.api.caching.CachableItem;
import com.oberasoftware.jasdb.api.exceptions.CoreConfigException;

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
