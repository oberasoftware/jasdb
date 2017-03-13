package com.oberasoftware.jasdb.core.caching;

import com.oberasoftware.jasdb.api.caching.CachableItem;

public class CacheElement {
	private String cachingKey;
	private CachableItem cachedItem;
	private long entryId;
	
	protected CacheElement(String cachingKey, CachableItem cachedItem) {
		this.cachedItem = cachedItem;
		this.cachingKey = cachingKey;
		this.entryId = System.currentTimeMillis();
	}

	@Override
	public boolean equals(Object obj) {
		if(obj instanceof CacheElement) {
			return ((CacheElement)obj).cachingKey.equals(cachingKey);
		} else {
			return false;
		}
	}
	
	public long getEntryId() {
		return this.entryId;
	}
	
	public void setEntryId(long entryId) {
		this.entryId = entryId;
	}
	
	public CachableItem getCachedItem() {
		return this.cachedItem;
	}
	
	public String getKey() {
		return this.cachingKey;
	}

	@Override
	public int hashCode() {
		return cachingKey.hashCode();
	}
}
