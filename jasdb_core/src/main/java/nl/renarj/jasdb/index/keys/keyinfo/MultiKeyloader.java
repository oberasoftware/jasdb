/*
 * The JASDB software and code is Copyright protected 2011 and owned by Renze de Vries
 * 
 * All the code and design principals in the codebase are also Copyright 2011 
 * protected and owned Renze de Vries. Any unauthorized usage of the code or the 
 * design and principals as in this code is prohibited.
 */
package nl.renarj.jasdb.index.keys.keyinfo;

import nl.renarj.jasdb.core.IndexableItem;
import nl.renarj.jasdb.core.exceptions.JasDBStorageException;
import nl.renarj.jasdb.core.storage.datablocks.DataBlock;
import nl.renarj.jasdb.index.keys.Key;
import nl.renarj.jasdb.index.keys.factory.KeyFactory;

import java.nio.ByteBuffer;
import java.util.List;

public interface MultiKeyloader {

	int getKeySize();

    int getMemorySize();

	void loadKeys(Key targetKey, int offset, ByteBuffer keyBuffer) throws JasDBStorageException;

	void writeKeys(Key sourceKey, int offset, ByteBuffer keyBuffer) throws JasDBStorageException;

    KeyLoadResult loadKeys(Key targetKey, int offset, DataBlock dataBlock) throws JasDBStorageException;

    DataBlock writeKeys(Key sourceKey, DataBlock dataBlock) throws JasDBStorageException;

	void enrichKey(IndexableItem indexableItem, Key key) throws JasDBStorageException;

    KeyFactory[] getKeyFactories();

    KeyNameMapper getKeyNameMapper();

    List<String> getFields();
	
	String asHeader();

}