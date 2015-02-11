/*
 * The JASDB software and code is Copyright protected 2011 and owned by Renze de Vries
 * 
 * All the code and design principals in the codebase are also Copyright 2011 
 * protected and owned Renze de Vries. Any unauthorized usage of the code or the 
 * design and principals as in this code is prohibited.
 */
package nl.renarj.jasdb.index.keys.factory;

import nl.renarj.jasdb.core.IndexableItem;
import nl.renarj.jasdb.core.exceptions.JasDBStorageException;
import nl.renarj.jasdb.core.storage.datablocks.DataBlock;
import nl.renarj.jasdb.index.keys.Key;
import nl.renarj.jasdb.index.keys.keyinfo.KeyLoadResult;
import nl.renarj.jasdb.index.keys.types.KeyType;

import java.nio.ByteBuffer;
import java.util.Set;

public interface KeyFactory {
	Key loadKey(int curPosition, ByteBuffer byteBuffer) throws JasDBStorageException;
	void writeKey(Key key, int curPosition, ByteBuffer byteBuffer) throws JasDBStorageException;

    KeyLoadResult loadKey(int offset, DataBlock dataBlock) throws JasDBStorageException;
    DataBlock writeKey(Key key, DataBlock dataBlock) throws JasDBStorageException;

	Key convertKey(Key key) throws JasDBStorageException;
	boolean supportsKey(Key key);
	
	Key createKey(IndexableItem indexableItem) throws JasDBStorageException;
	Key createEmptyKey();
    Set<Key> createMultivalueKeys(IndexableItem indexableItem) throws JasDBStorageException;

    KeyType getKeyType();
	String asHeader();

	String getKeyId();
	String getFieldName();

    int getKeySize();
    int getMemorySize();

    boolean isMultiValueKey(IndexableItem indexableItem) throws JasDBStorageException;
}
