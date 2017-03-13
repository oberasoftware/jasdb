package com.oberasoftware.jasdb.core.index.keys.factory;

import com.oberasoftware.jasdb.api.index.keys.KeyFactory;
import com.oberasoftware.jasdb.api.session.IndexableItem;
import com.oberasoftware.jasdb.api.exceptions.JasDBStorageException;
import com.oberasoftware.jasdb.api.storage.DataBlock;
import com.oberasoftware.jasdb.api.index.keys.Key;
import com.oberasoftware.jasdb.api.index.keys.KeyLoadResult;
import com.oberasoftware.jasdb.core.index.keys.keyinfo.MultiKeyloader;
import com.oberasoftware.jasdb.api.index.keys.KeyType;

import java.nio.ByteBuffer;
import java.util.Set;

public class WrappedValueKeyFactory implements KeyFactory {
	private KeyFactory wrappedKeyFactory;
	private MultiKeyloader valueLoader;
	
	public WrappedValueKeyFactory(KeyFactory originalKeyFactory, MultiKeyloader valueLoader) {
		this.wrappedKeyFactory = originalKeyFactory;
		this.valueLoader = valueLoader;
	}

	@Override
	public Key loadKey(int curPosition, ByteBuffer byteBuffer) throws JasDBStorageException {
		return wrappedKeyFactory.loadKey(curPosition, byteBuffer);
	}

	@Override
	public void writeKey(Key key, int curPosition, ByteBuffer byteBuffer) throws JasDBStorageException {
		wrappedKeyFactory.writeKey(key, curPosition, byteBuffer);
	}

    @Override
    public KeyLoadResult loadKey(int offset, DataBlock dataBlock) throws JasDBStorageException {
        return wrappedKeyFactory.loadKey(offset, dataBlock);
    }

    @Override
    public DataBlock writeKey(Key key, DataBlock dataBlock) throws JasDBStorageException {
        return wrappedKeyFactory.writeKey(key, dataBlock);
    }

    @Override
	public Key createKey(IndexableItem indexableItem) throws JasDBStorageException {
		Key createdKey = wrappedKeyFactory.createKey(indexableItem);
		this.valueLoader.enrichKey(indexableItem, createdKey);
		
		return createdKey;
	}

	@Override
	public Key createEmptyKey() {
		return wrappedKeyFactory.createEmptyKey();
	}

	@Override
    public Set<Key> createMultivalueKeys(IndexableItem indexableItem) throws JasDBStorageException {
        Set<Key> keys = wrappedKeyFactory.createMultivalueKeys(indexableItem);
        for(Key key : keys) {
            this.valueLoader.enrichKey(indexableItem, key);
        }
        return keys;
    }

    @Override
    public boolean isMultiValueKey(IndexableItem indexableItem) throws JasDBStorageException {
        return wrappedKeyFactory.isMultiValueKey(indexableItem);
    }

    @Override
	public Key convertKey(Key key) throws JasDBStorageException {
		return wrappedKeyFactory.convertKey(key);
	}

	@Override
	public boolean supportsKey(Key key) {
		return wrappedKeyFactory.supportsKey(key);
	}

	@Override
	public String asHeader() {
		return wrappedKeyFactory.asHeader();
	}

    @Override
    public KeyType getKeyType() {
        return wrappedKeyFactory.getKeyType();
    }

    @Override
	public int getKeySize() {
		return wrappedKeyFactory.getKeySize();
	}

    @Override
    public int getMemorySize() {
        return wrappedKeyFactory.getMemorySize() + valueLoader.getMemorySize();
    }

    @Override
	public String getKeyId() {
		return wrappedKeyFactory.getKeyId();
	}

	@Override
	public String getFieldName() {
		return wrappedKeyFactory.getFieldName();
	}
}
