/*
 * The JASDB software and code is Copyright protected 2011 and owned by Renze de Vries
 * 
 * All the code and design principals in the codebase are also Copyright 2011 
 * protected and owned Renze de Vries. Any unauthorized usage of the code or the 
 * design and principals as in this code is prohibited.
 */
package com.oberasoftware.jasdb.api.index.keys;

import java.util.Map;

/**
 * The key represents a search item in the index, the index stores the key and allows it to be retrieved. The key
 * can contain a payload consisting of multiple subkeys which are not part of the index but can be quickly retrieved.
 * The index will allow storing of any key type by implementing this interface for both values and the key itself. The
 * key needs to be comparable which is used to apply a Lexicographical order comparison with order keys so the btree
 * can be used for quick search and storing. 
 * 
 * @author Renze de Vries
 *
 */
public interface Key extends ComparableKey<Key> {
	int hashCode();
	boolean equals(Object obj);
	String toString();

	/**
	 * This retrieves a  map of all the child keys stored in this key
	 * @return The map containing all the value payload keys
	 */
	Map<String, Key> getKeysByName(KeyNameMapper keyMapper);

    /**
     * Returns an array of the child keys
     * @return The keys in this key
     */
    Key[] getKeys();

    /**
     * Returns if the key has children
     * @return True if the key has children, False if not
     */
    boolean hasChildren();

//    CompositeKey getChildrenAsComposite();
	
	/**
	 * This retrieves a specific payload value KeyValue object which contains the Key of the value
	 * 
	 * @param keyMapper the mapper to map from name to index
     * @param name The name of the key field to retrieve
	 * @return The Key requested for the given name, null if not present
	 */
	Key getKey(KeyNameMapper keyMapper, String name);

    /**
     * This retrieves a key at a given index
     * @param index The index at which the key is held
     * @return The key if present, null if not
     */
    Key getKey(int index);
	
	/**
	 * This adds a child Key to the key, in case the mapper cannot map will not add to child keys
     * and just ignore the added key
     *
	 * @param key The child Key to add to the payload of this key
     * @param keyMapper the mapper to map from name to index
     * @param name The name that represents the field / key
	 * @return This key will be returned for easy payload value building (fluent api)
	 */
	Key addKey(KeyNameMapper keyMapper, String name, Key key);
	
	/**
	 * This sets all the key payload values
     *
     * @param keyMapper the mapper to map from name to index
	 * @param keys the child keys to add
	 * @return This key will be returned for easy payload value building (fluent api) 
	 */
	Key setKeys(KeyNameMapper keyMapper, Map<String, Key> keys);

    /**
     * Sets directly the children key values
     * @param keys The keys
     * @return This key will be returned for easy payload value building (fluent api)
     */
    Key setKeys(Key[] keys);
	
	/**
	 * Clone the current key without influencing this instance
	 * @return The cloned Key including payload values
	 */
	Key cloneKey();
	
	/**
	 * Clone the current key without influencing this instance, whilst leaving the values
	 * @param includeChildren True if child keys need to be included;
	 * @return The key with values if was specified True, False means without values
	 */
	Key cloneKey(boolean includeChildren);

    long size();

    int getKeyCount();

    Object getValue();
}
