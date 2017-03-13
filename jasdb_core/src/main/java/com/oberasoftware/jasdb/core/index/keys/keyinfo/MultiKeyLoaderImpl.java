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
import java.util.ArrayList;
import java.util.List;


public class MultiKeyLoaderImpl implements MultiKeyloader {
    private KeyFactory[] keyFactories;
    private KeyNameMapper keyNameMapper;

	private int diskSize = 0;
    private int memorySize = 0;
    private List<String> fields;

	public MultiKeyLoaderImpl(KeyNameMapper keyNameMapper, KeyFactory[] keyFactories) {
        this.keyNameMapper = keyNameMapper;
		this.keyFactories = keyFactories;
		calculateKeySize();

        this.fields = new ArrayList<>();
        for(KeyFactory keyFactory : keyFactories) {
            this.fields.add(keyFactory.getFieldName());
        }
	}
	
	private void calculateKeySize() {
		for(KeyFactory keyFactory : keyFactories) {
			diskSize += keyFactory.getKeySize();
            memorySize += keyFactory.getMemorySize();
		}
	}

	public int getKeySize() {
		return diskSize;
	}

    @Override
    public int getMemorySize() {
        return memorySize;
    }

    @Override
    public List<String> getFields() {
        return this.fields;
    }

    @Override
	public String asHeader() {
		StringBuilder headerBuilder = new StringBuilder();
		for(KeyFactory keyFactory : keyFactories) {
			headerBuilder.append(keyFactory.asHeader());
		}
		return headerBuilder.toString();
	}

    @Override
	public void loadKeys(Key targetKey, int offset, ByteBuffer keyBuffer) throws JasDBStorageException {
		int curPosition = offset;
		for(KeyFactory keyFactory : keyFactories) {
			Key valueKey = keyFactory.loadKey(curPosition, keyBuffer);
			targetKey.addKey(keyNameMapper, keyFactory.getFieldName(), valueKey);
			
			curPosition += keyFactory.getKeySize();
		}
	}

    @Override
	public void writeKeys(Key sourceKey, int offset, ByteBuffer keyBuffer) throws JasDBStorageException {
		int curPosition = offset;
		for(KeyFactory keyFactory : keyFactories) {
            Key value = sourceKey.getKey(keyNameMapper, keyFactory.getFieldName());

			if(value != null) {
				keyFactory.writeKey(value, curPosition, keyBuffer);
			}
			
			curPosition += keyFactory.getKeySize();
		}
	}

    @Override
    public KeyLoadResult loadKeys(Key targetKey, int offset, DataBlock dataBlock) throws JasDBStorageException {
        int curOffset = offset;
        DataBlock currentBlock = dataBlock;
        for(KeyFactory keyFactory : keyFactories) {
            KeyLoadResult valueKeyResult = keyFactory.loadKey(curOffset, currentBlock);
            targetKey.addKey(keyNameMapper, keyFactory.getFieldName(), valueKeyResult.getLoadedKey());

            currentBlock = valueKeyResult.getEndBlock();
            curOffset = valueKeyResult.getNextOffset();
        }

        return new KeyLoadResultImpl(targetKey, currentBlock, curOffset);
    }

    @Override
    public DataBlock writeKeys(Key sourceKey, DataBlock dataBlock) throws JasDBStorageException {
        DataBlock currentBlock = dataBlock;
        for(KeyFactory keyFactory : keyFactories) {
            Key value = sourceKey.getKey(keyNameMapper, keyFactory.getFieldName());

            if(value != null) {
                currentBlock = keyFactory.writeKey(value, currentBlock);
            } else {
                throw new JasDBStorageException("Cannot insert key into index, field: " + keyFactory.getFieldName() + " missing in key: " + sourceKey);
            }
        }
        return currentBlock;
    }

    @Override
    public KeyFactory[] getKeyFactories() {
        return keyFactories;
    }

    @Override
    public KeyNameMapper getKeyNameMapper() {
        return keyNameMapper;
    }

    @Override
	public void enrichKey(IndexableItem indexableItem, Key key) throws JasDBStorageException {
		for(KeyFactory keyFactory : keyFactories) {
			Key createdKey = keyFactory.createKey(indexableItem);
            key.addKey(keyNameMapper, keyFactory.getFieldName(), createdKey);
		}
	}
}
