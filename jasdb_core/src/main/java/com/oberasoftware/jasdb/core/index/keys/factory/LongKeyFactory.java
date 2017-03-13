/*
 * The JASDB software and code is Copyright protected 2011 and owned by Renze de Vries
 * 
 * All the code and design principals in the codebase are also Copyright 2011 
 * protected and owned Renze de Vries. Any unauthorized usage of the code or the 
 * design and principals as in this code is prohibited.
 */
package com.oberasoftware.jasdb.core.index.keys.factory;

import com.oberasoftware.jasdb.api.index.keys.KeyFactory;
import com.oberasoftware.jasdb.api.session.IndexableItem;
import com.oberasoftware.jasdb.api.exceptions.JasDBStorageException;
import com.oberasoftware.jasdb.api.storage.DataBlock;
import com.oberasoftware.jasdb.api.storage.DataBlockResult;
import com.oberasoftware.jasdb.api.index.MemoryConstants;
import com.oberasoftware.jasdb.api.index.keys.Key;
import com.oberasoftware.jasdb.core.index.keys.LongKey;
import com.oberasoftware.jasdb.core.index.keys.StringKey;
import com.oberasoftware.jasdb.api.index.keys.KeyLoadResult;
import com.oberasoftware.jasdb.api.index.keys.KeyType;
import com.oberasoftware.jasdb.core.index.keys.keyinfo.KeyLoadResultImpl;
import com.oberasoftware.jasdb.core.index.keys.types.LongKeyType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

public class LongKeyFactory extends AbstractKeyFactory implements KeyFactory {
	private static final int KEY_SIZE = MemoryConstants.LONG_BYTE_SIZE;
    private static final int MEMORY_SIZE = MemoryConstants.LONG_BYTE_SIZE + MemoryConstants.OBJECT_BYTE_SIZE;
	
	private Logger log = LoggerFactory.getLogger(LongKeyFactory.class);
	
	private String field;
	
	public LongKeyFactory(String field) {
        super(field);
		this.field = field;
	}
	
	@Override
	public String asHeader() {
		return field + "(" + getKeyId() + ");";
	}

	@Override
	public Key loadKey(int curPosition, ByteBuffer byteBuffer) throws JasDBStorageException {
		long keyValue = byteBuffer.getLong(curPosition);
		
		if(keyValue != -1) {
			return new LongKey(keyValue);
		} else {
			return null;
		}
	}

	@Override
	public void writeKey(Key key, int curPosition, ByteBuffer byteBuffer) throws JasDBStorageException {
		if(key instanceof LongKey) {
			LongKey longKey = (LongKey) key;
			
			byteBuffer.putLong(curPosition, longKey.getKey());
		} else {
			throw new JasDBStorageException("The key is of an unexpected type: " + key.getClass().toString());
		}
	}

    @Override
    public KeyLoadResult loadKey(int offset, DataBlock dataBlock) throws JasDBStorageException {
        DataBlockResult<Long> loadResult = dataBlock.loadLong(offset);
        if(loadResult != null) {
            return new KeyLoadResultImpl(new LongKey(loadResult.getValue()), loadResult.getEndBlock(), loadResult.getNextOffset());
        } else {
            return null;
        }
    }

    @Override
    public DataBlock writeKey(Key key, DataBlock dataBlock) throws JasDBStorageException {
        if(key instanceof LongKey) {
            LongKey longKey = (LongKey) key;
            return dataBlock.writeLong(longKey.getKey()).getDataBlock();
        } else {
            throw new JasDBStorageException("The key is of an unexpected type: " + key.getClass().toString());
        }
    }

    @Override
	public Key createKey(IndexableItem indexableItem) throws JasDBStorageException {
		Object value = indexableItem.getValue(field);
        return convertToKey(value);
	}

	@Override
	public Key createEmptyKey() {
		return new LongKey(new byte[0]);
	}

	@Override
    protected Key convertToKey(Object value) throws JasDBStorageException {
        if(value != null) {
            if(value instanceof Long) {
                return new LongKey((Long) value);
            } else if(value instanceof Integer) {
                return new LongKey((Integer) value);
            } else if(value instanceof String) {
                try {
                    return new LongKey(Long.parseLong((String) value));
                } catch(NumberFormatException e) {
                    log.debug("Invalid value for index was passed");
                }
            }
        } else {
			return new LongKey(new byte[0]);
		}

        throw new JasDBStorageException("Unable to create key for field: " + this.field);
    }

    @Override
	public Key convertKey(Key key) throws JasDBStorageException {
		if(key instanceof LongKey) {
			return key;
		} else if(key instanceof StringKey) {
			StringKey stringKey = (StringKey) key;
			try {
				Long parsedLong = Long.parseLong(stringKey.getKey());
				return new LongKey(parsedLong);
			} catch(NumberFormatException e) {
				throw new JasDBStorageException("Unable to convert String to Long");
			}
		} else {
			//unsupported conversion
			return key;
		}
	}

	@Override
	public boolean supportsKey(Key key) {
		return key instanceof LongKey;
	}

	@Override
	public String getKeyId() {
		return LongKeyType.KEY_ID;
	}

    @Override
    public KeyType getKeyType() {
        return new LongKeyType();
    }

    @Override
	public int getKeySize() {
		return KEY_SIZE;
	}

    @Override
    public int getMemorySize() {
        return MEMORY_SIZE;
    }

    @Override
	public String getFieldName() {
		return this.field;
	}
}
