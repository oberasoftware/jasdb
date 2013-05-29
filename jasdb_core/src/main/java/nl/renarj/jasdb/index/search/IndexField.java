/*
 * The JASDB software and code is Copyright protected 2011 and owned by Renze de Vries
 * 
 * All the code and design principals in the codebase are also Copyright 2011 
 * protected and owned Renze de Vries. Any unauthorized usage of the code or the 
 * design and principals as in this code is prohibited.
 */
package nl.renarj.jasdb.index.search;

import nl.renarj.jasdb.index.keys.types.KeyType;

/**
 * This represents a field that needs to be indexed.
 */
public class IndexField {
	private String field;
	private KeyType keyType;

    /**
     * Creates an indexable field
     * @param field The field that needs to be indexed
     * @param keyType The type of the key in the index
     */
	public IndexField(String field, KeyType keyType) {
		this.field = field;
		this.keyType = keyType;
	}

    /**
     * Gets the field that needs to be indexed
     * @return The field name to be index
     */
	public String getField() {
		return field;
	}

    /**
     * Gets the key type
     * @return The key type
     */
	public KeyType getKeyType() {
		return keyType;
	}
}
