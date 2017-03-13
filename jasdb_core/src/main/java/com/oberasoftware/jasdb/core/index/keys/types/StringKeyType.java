/*
 * The JASDB software and code is Copyright protected 2011 and owned by Renze de Vries
 * 
 * All the code and design principals in the codebase are also Copyright 2011 
 * protected and owned Renze de Vries. Any unauthorized usage of the code or the 
 * design and principals as in this code is prohibited.
 */
package com.oberasoftware.jasdb.core.index.keys.types;

import com.oberasoftware.jasdb.api.index.keys.KeyType;

public class StringKeyType implements KeyType {
	public static final String KEY_ID = "stringType";
	public static final int DEFAULT_MAX_SIZE = 1024;
	
	private int maxSize;
	
	public StringKeyType() {
		this.maxSize = DEFAULT_MAX_SIZE;
	}
	
	public StringKeyType(int maxSize) {
		this.maxSize = maxSize;
	}

    public int getMaxSize() {
        return maxSize;
    }

    @Override
	public String getKeyId() {
		return KEY_ID;
	}

	@Override
	public String[] getKeyArguments() {
		return new String[] { String.valueOf(maxSize) };
	}
	
	
}
