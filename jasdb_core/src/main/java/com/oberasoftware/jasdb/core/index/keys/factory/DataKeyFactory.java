package com.oberasoftware.jasdb.core.index.keys.factory;

import com.oberasoftware.jasdb.api.exceptions.JasDBStorageException;
import com.oberasoftware.jasdb.api.index.keys.Key;
import com.oberasoftware.jasdb.api.index.keys.KeyLoadResult;
import com.oberasoftware.jasdb.api.index.keys.KeyType;
import com.oberasoftware.jasdb.api.session.IndexableItem;
import com.oberasoftware.jasdb.api.storage.ClonableDataStream;
import com.oberasoftware.jasdb.api.storage.DataBlock;
import com.oberasoftware.jasdb.api.storage.DataBlockResult;
import com.oberasoftware.jasdb.core.index.keys.DataKey;
import com.oberasoftware.jasdb.core.index.keys.keyinfo.KeyLoadResultImpl;
import com.oberasoftware.jasdb.core.index.keys.types.DataKeyType;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

/**
 * @author Renze de Vries
 */
public class DataKeyFactory extends AbstractKeyFactory {
    public DataKeyFactory(String field) {
        super(field);
    }

    @Override
    protected Key convertToKey(Object value) throws JasDBStorageException {
        return null;
    }

    @Override
    public Key loadKey(int curPosition, ByteBuffer byteBuffer) throws JasDBStorageException {
        return null;
    }

    @Override
    public void writeKey(Key key, int curPosition, ByteBuffer byteBuffer) throws JasDBStorageException {

    }

    @Override
    public KeyLoadResult loadKey(int offset, DataBlock dataBlock) throws JasDBStorageException {
        DataBlockResult<ClonableDataStream> loadResult = dataBlock.loadStream(offset);

        return new KeyLoadResultImpl(new DataKey(loadResult.getValue()), loadResult.getEndBlock(), loadResult.getNextOffset());
    }

    @Override
    public DataBlock writeKey(Key key, DataBlock dataBlock) throws JasDBStorageException {
        if(key instanceof DataKey) {
            DataKey dataKey = (DataKey) key;

            try {
                InputStream inputStream = dataKey.getStream();
                inputStream.reset();
                DataBlock.WriteResult writeResult = dataBlock.writeStream(inputStream);

                return writeResult.getDataBlock();
            } catch(IOException e) {
                throw new JasDBStorageException("Unable to read from input stream, not reusable stream", e);
            }
        } else {
            throw new JasDBStorageException("The key is of an unexpected type: " + key.getClass().toString());
        }
    }

    @Override
    public Key convertKey(Key key) throws JasDBStorageException {
        throw new JasDBStorageException("Unable to create key for field: " + getField());
    }

    @Override
    public boolean supportsKey(Key key) {
        return key instanceof DataKey;
    }

    @Override
    public Key createKey(IndexableItem indexableItem) throws JasDBStorageException {
        return null;
    }

    @Override
    public Key createEmptyKey() {
        return null;
    }

    @Override
    public KeyType getKeyType() {
        return new DataKeyType();
    }

    @Override
    public String asHeader() {
        return getFieldName() + "(" + getKeyId() + ");";
    }

    @Override
    public String getKeyId() {
        return DataKeyType.KEY_ID;
    }

    @Override
    public String getFieldName() {
        return getField();
    }

    @Override
    public int getKeySize() {
        return 0;
    }

    @Override
    public int getMemorySize() {
        return 0;
    }
}
