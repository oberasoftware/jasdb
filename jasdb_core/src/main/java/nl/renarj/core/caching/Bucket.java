package nl.renarj.core.caching;

import nl.renarj.core.exceptions.CoreConfigException;

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