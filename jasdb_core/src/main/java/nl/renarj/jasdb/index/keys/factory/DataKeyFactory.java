package nl.renarj.jasdb.index.keys.factory;

import nl.renarj.jasdb.core.IndexableItem;
import nl.renarj.jasdb.core.exceptions.JasDBStorageException;
import nl.renarj.jasdb.core.storage.datablocks.DataBlock;
import nl.renarj.jasdb.core.storage.datablocks.DataBlockResult;
import nl.renarj.jasdb.core.storage.datablocks.impl.BlockDataInputStream;
import nl.renarj.jasdb.index.keys.Key;
import nl.renarj.jasdb.index.keys.impl.DataKey;
import nl.renarj.jasdb.index.keys.keyinfo.KeyLoadResult;
import nl.renarj.jasdb.index.keys.types.DataKeyType;
import nl.renarj.jasdb.index.keys.types.KeyType;

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
        DataBlockResult<BlockDataInputStream> loadResult = dataBlock.loadStream(offset);

        return new KeyLoadResult(new DataKey(loadResult.getValue()), loadResult.getEndBlock(), loadResult.getNextOffset());
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
        throw new JasDBStorageException("Unable to create key for field: " + this.field);
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
    public KeyType getKeyType() {
        return new DataKeyType();
    }

    @Override
    public String asHeader() {
        StringBuilder headerBuilder = new StringBuilder();
        headerBuilder.append(getFieldName()).append("(");
        headerBuilder.append(getKeyId()).append(");");

        return headerBuilder.toString();
    }

    @Override
    public String getKeyId() {
        return DataKeyType.KEY_ID;
    }

    @Override
    public String getFieldName() {
        return this.field;
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
