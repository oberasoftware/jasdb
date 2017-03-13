package com.oberasoftware.jasdb.api.caching;

import com.oberasoftware.jasdb.api.model.CacheConfig;
import com.oberasoftware.jasdb.api.caching.CachableItem;
import com.oberasoftware.jasdb.api.exceptions.CoreConfigException;

/**
 * 
 * @author Renze de Vries
 *
 */
public interface Bucket {
	String getName();

	void configure(CacheConfig config) throws CoreConfigException;

	void put(String key, CachableItem cacheItem);

	void remove(String key);

	boolean containsItem(String key);

	CachableItem getItem(String key);

	long getMemSize();

	int getCachedItems();
	
	void closeBucket();

}