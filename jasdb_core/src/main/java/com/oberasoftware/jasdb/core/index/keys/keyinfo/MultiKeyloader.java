/*
 * The JASDB software and code is Copyright protected 2011 and owned by Renze de Vries
 * 
 * All the code and design principals in the codebase are also Copyright 2011 
 * protected and owned Renze de Vries. Any unauthorized usage of the code or the 
 * design and principals as in this code is prohibited.
 */
package com.oberasoftware.jasdb.core.index.keys.keyinfo;

import com.oberasoftware.jasdb.api.index.keys.KeyLoadResult;
import com.oberasoftware.jasdb.api.index.keys.KeyNameMapper;
import com.oberasoftware.jasdb.api.session.IndexableItem;
import com.oberasoftware.jasdb.api.exceptions.JasDBStorageException;
import com.oberasoftware.jasdb.api.storage.DataBlock;
import com.oberasoftware.jasdb.api.index.keys.Key;
import com.oberasoftware.jasdb.api.index.keys.KeyFactory;

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