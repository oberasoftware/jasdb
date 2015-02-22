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
import nl.renarj.jasdb.core.storage.datablocks.DataBlockResult;
import nl.renarj.jasdb.index.MemoryConstants;
import nl.renarj.jasdb.index.keys.Key;
import nl.renarj.jasdb.index.keys.impl.StringKey;
import nl.renarj.jasdb.index.keys.impl.UUIDKey;
import nl.renarj.jasdb.index.keys.keyinfo.KeyLoadResult;
import nl.renarj.jasdb.index.keys.types.KeyType;
import nl.renarj.jasdb.index.keys.types.UUIDKeyType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.UUID;

public class UUIDKeyFactory extends AbstractKeyFactory implements KeyFactory {
	private static final int KEY_SIZE = ((Long.SIZE * 2) / Byte.SIZE);
    private static final int MEMORY_SIZE = (MemoryConstants.LONG_BYTE_SIZE * 2) + MemoryConstants.OBJECT_BYTE_SIZE;
	
	private static final Logger LOG = LoggerFactory.getLogger(UUIDKeyFactory.class);
	
	private String field;
	
	public UUIDKeyFactory(String field) {
        super(field);
		this.field = field;
	}
	
	@Override
	public String asHeader() {
        return field + "(" + getKeyId() + ");";
	}

	@Override
	public Key loadKey(int curPosition, ByteBuffer byteBuffer) throws JasDBStorageException {
		long mostSignificantBits = byteBuffer.getLong(curPosition);
		long leastSignificantBits = byteBuffer.getLong(curPosition + (Long.SIZE / Byte.SIZE));
		
		return new UUIDKey(leastSignificantBits, mostSignificantBits);
	}

	@Override
	public void writeKey(Key key, int curPosition, ByteBuffer byteBuffer) throws JasDBStorageException {
		if(key instanceof UUIDKey) {
			UUIDKey uuidKey = (UUIDKey) key;
			
			byteBuffer.putLong(curPosition, uuidKey.getMostSignificant());
			byteBuffer.putLong(curPosition + (Long.SIZE / Byte.SIZE), uuidKey.getLeastSignificant());
		} else {
			throw new JasDBStorageException("The key is of an unexpected type: " + key.getClass().toString());
		}
	}

    @Override
    public KeyLoadResult loadKey(int offset, DataBlock dataBlock) throws JasDBStorageException {
        DataBlockResult<Long> mostSignificantBitsResult = dataBlock.loadLong(offset);
        DataBlockResult<Long> leastSignificantBitsResult = mostSignificantBitsResult.getEndBlock().loadLong(mostSignificantBitsResult.getNextOffset());

        return new KeyLoadResult(new UUIDKey(leastSignificantBitsResult.getValue(),
                mostSignificantBitsResult.getValue()), leastSignificantBitsResult.getEndBlock(), leastSignificantBitsResult.getNextOffset());
    }

    @Override
    public DataBlock writeKey(Key key, DataBlock dataBlock) throws JasDBStorageException {
        if(key instanceof UUIDKey) {
            UUIDKey uuidKey = (UUIDKey) key;

            dataBlock = dataBlock.writeLong(uuidKey.getMostSignificant()).getDataBlock();
            dataBlock = dataBlock.writeLong(uuidKey.getLeastSignificant()).getDataBlock();
            return dataBlock;
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
        return null;
    }

    @Override
    protected Key convertToKey(Object value) throws JasDBStorageException {
        if(value != null) {
            if(value instanceof String) {
                try {
                    UUID parsedUUID = UUID.fromString(value.toString());
                    return new UUIDKey(parsedUUID.getLeastSignificantBits(), parsedUUID.getMostSignificantBits());
                } catch(IllegalArgumentException e) {
                    LOG.debug("Invalid value for index was passed");
                }
            }
        }

        throw new JasDBStorageException("Unable to create key for field: " + this.field);
    }

    @Override
	public Key convertKey(Key key) throws JasDBStorageException {
        if(key instanceof UUIDKey) {
            return key;
        } else if(key instanceof StringKey) {
            return new UUIDKey(UUID.fromString((String) key.getValue()));
        } else {
            //unsupported conversion
		    return key;
        }
	}

	@Override
	public boolean supportsKey(Key key) {
        return key instanceof UUIDKey;
	}

	@Override
	public String getKeyId() {
		return UUIDKeyType.KEY_ID;
	}

    @Override
    public KeyType getKeyType() {
        return new UUIDKeyType();
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
