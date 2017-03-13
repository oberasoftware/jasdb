/*
 * The JASDB software and code is Copyright protected 2011 and owned by Renze de Vries
 * 
 * All the code and design principals in the codebase are also Copyright 2011 
 * protected and owned Renze de Vries. Any unauthorized usage of the code or the 
 * design and principals as in this code is prohibited.
 */
package com.oberasoftware.jasdb.api.index.keys;

import com.oberasoftware.jasdb.api.exceptions.JasDBStorageException;
import com.oberasoftware.jasdb.api.index.IndexField;
import com.oberasoftware.jasdb.api.storage.DataBlock;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Set;

/**
 * This represents all the key information in an index
 * 
 * @author Renze de Vries
 *
 */
public interface KeyInfo {
	Key loadKey(int curPosition, ByteBuffer byteBuffer) throws JasDBStorageException;
	void writeKey(Key key, int curPosition, ByteBuffer byteBuffer) throws JasDBStorageException;

    KeyLoadResult loadKey(int curPosition, DataBlock dataBlock) throws JasDBStorageException;
    DataBlock writeKey(Key key, DataBlock dataBlock) throws JasDBStorageException;

	/**
	 * Persists all key information as a header string
	 * @return The key information as header string
	 */
	String keyAsHeader();
	
	/**
	 * Persists all value information as a header string
	 * @return The value information as header string
	 */
	String valueAsHeader();
	
	/**
	 * Determines how close the gives fields match the index descriptor. The scale is on 0-200. This would
	 * reach 200 in case all value fields and index fields are a 100% match.
	 * The calculation is N (number of matching fields) / field.size + (N / fields.size) * included matching
	 * columns. 
	 *  
	 * @param fields The fields to check for matching
	 * @return The matching on a scale from 0-200
	 */
	int match(Set<String> fields);
	
	/**
	 * This determines the byte size that is required to store all the key information in the index
	 * @return The bytesize required to store all the key information
	 */
	int getKeySize();
	
	/**
	 * Returns a name of the key field(s)
	 * @return The name of the key field(s)
	 */
	String getKeyName();
	
    List<IndexField> getIndexKeyFields();

    List<IndexField> getIndexValueFields();

	/**
	 * Returns all the fields kept in the key part of the index
	 * @return The list of all the key fields in the index
	 */
	List<String> getKeyFields();
	
	List<String> getValueFields();
	
	/**
	 * The key factory used to create/serialize/deserialize keys for the index
	 * @return The keyfactory used for this index
	 */
	KeyFactory getKeyFactory();

    /**
     * Gets the key name mapper which can be used to map the name of the
     * field to a position in the key payload.
     * @return The key name mapper
     */
    KeyNameMapper getKeyNameMapper();
}
