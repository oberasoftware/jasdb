package com.oberasoftware.jasdb.core.caching;

import com.oberasoftware.jasdb.api.caching.CachableItem;

public class CacheItemWrapper implements CachableItem {
	private long objectSize;
	private Object value;
	
	public CacheItemWrapper(Object value, long objectSize) {
		this.value = value;
		this.objectSize = objectSize;
	}

	public Object getValue() {
		return value;
	}

	public void setObjectSize(long objectSize) {
		this.objectSize = objectSize;
	}

	@Override
	public long getObjectSize() {
		return this.objectSize;
	}
}
