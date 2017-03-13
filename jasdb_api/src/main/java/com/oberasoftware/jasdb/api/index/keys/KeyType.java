/*
 * The JASDB software and code is Copyright protected 2011 and owned by Renze de Vries
 * 
 * All the code and design principals in the codebase are also Copyright 2011 
 * protected and owned Renze de Vries. Any unauthorized usage of the code or the 
 * design and principals as in this code is prohibited.
 */
package com.oberasoftware.jasdb.api.index.keys;

/**
 * This represents the key types available for indexing. All indexable
 * keys are of a type, by default there are the following keys available:
 *
 */
public interface KeyType {
    /**
     * An id representing the type of key
     * @return The id of the key type
     */
	String getKeyId();

    /**
     * The arguments needed to create the key factory
     * @return The arguments needed for the key factory
     */
	String[] getKeyArguments();
}
