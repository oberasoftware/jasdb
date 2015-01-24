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
import nl.renarj.jasdb.index.keys.impl.LongKey;
import nl.renarj.jasdb.index.keys.impl.StringKey;
import nl.renarj.jasdb.index.keys.keyinfo.KeyLoadResult;
import nl.renarj.jasdb.index.keys.types.KeyType;
import nl.renarj.jasdb.index.keys.types.StringKeyType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

public class StringKeyFactory extends AbstractKeyFactory implements KeyFactory {
	private static final Logger LOG = LoggerFactory.getLogger(StringKeyFactory.class);
	
	private String field;
	private int maxSize;
    private int memorySize;
	
	public StringKeyFactory(String field, String maxSize) {
        super(field);
		this.field = field;
		
		try {
			this.maxSize = Integer.parseInt(maxSize);
            this.memorySize = MemoryConstants.ARRAY_BYTE_SIZE + MemoryConstants.OBJECT_BYTE_SIZE + this.maxSize;
		} catch(NumberFormatException e) {
			LOG.error("Unable to determine max string key size", e);
		}
	}
	
	@Override
	public Key loadKey(int curPosition, ByteBuffer byteBuffer) throws JasDBStorageException {
		byte[] keyBuffer = new byte[maxSize];
		byteBuffer.position(curPosition);
		byteBuffer.get(keyBuffer, 0, maxSize);

        return new StringKey(keyBuffer);
	}

	@Override
	public void writeKey(Key key, int curPosition, ByteBuffer byteBuffer) throws JasDBStorageException {
		if(key instanceof StringKey) {
			StringKey stringKey = (StringKey) key;

            byte[] keyBytes = stringKey.getUnicodeBytes();
            if(keyBytes.length > maxSize) {
                throw new JasDBStorageException("Key is too big to be stored in index, byte size: " + keyBytes.length + " max allowed: " + maxSize);
            } else {
                byteBuffer.position(curPosition);
                byteBuffer.put(keyBytes);
            }
		} else {
			throw new JasDBStorageException("The key is of an unexpected type: " + key.getClass().toString());
		}
	}

    @Override
    public KeyLoadResult loadKey(int offset, DataBlock dataBlock) throws JasDBStorageException {
        DataBlockResult<byte[]> keyBuffer = dataBlock.loadBytes(offset);

        return new KeyLoadResult(new StringKey(keyBuffer.getValue()), keyBuffer.getEndBlock(), keyBuffer.getNextOffset());
    }

    @Override
    public DataBlock writeKey(Key key, DataBlock dataBlock) throws JasDBStorageException {
        if(key instanceof StringKey) {
            StringKey stringKey = (StringKey) key;

            byte[] keyBytes = stringKey.getUnicodeBytes();
            if(keyBytes.length > maxSize) {
                throw new JasDBStorageException("Key is too big to be stored in index, byte size: " + keyBytes.length + " max allowed: " + maxSize);
            } else {
                return dataBlock.writeBytes(keyBytes).getDataBlock();
            }
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
    protected Key convertToKey(Object value) throws JasDBStorageException {
        if(value != null) {
            String fieldValue = value.toString();
            StringKey key = new StringKey(fieldValue);
            if(key.getUnicodeBytes().length > maxSize) {
                throw new JasDBStorageException("Field size has been exceeded: " + key.getUnicodeBytes().length + " max length: " + maxSize);
            } else {
                return key;
            }
        } else {
            return new StringKey(new byte[0]);
        }
    }

    @Override
	public Key convertKey(Key key) throws JasDBStorageException {
		if(key instanceof LongKey) {
			return new StringKey(key.getValue().toString());
		} else {
			throw new JasDBStorageException("Unsupported conversion from: " + key.getClass().getName() + " to StringKey");
		}
	}

	@Override
	public boolean supportsKey(Key key) {
        return key instanceof StringKey;
	}

	@Override
	public String asHeader() {
		StringBuilder headerBuilder = new StringBuilder();
		headerBuilder.append(getFieldName()).append("(");
		headerBuilder.append(getKeyId()).append(":").append(maxSize).append(");");

		return headerBuilder.toString();
	}

    @Override
    public KeyType getKeyType() {
        return new StringKeyType(maxSize);
    }

    @Override
	public String getKeyId() {
		return StringKeyType.KEY_ID;
	}

	@Override
	public int getKeySize() {
		return this.maxSize;
	}

    @Override
    public int getMemorySize() {
        return memorySize;
    }

    @Override
	public String getFieldName() {
		return this.field;
	}
}
