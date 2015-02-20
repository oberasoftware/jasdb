package nl.renarj.jasdb.index.keys.factory;

import nl.renarj.jasdb.core.IndexableItem;
import nl.renarj.jasdb.core.exceptions.JasDBStorageException;
import nl.renarj.jasdb.core.storage.datablocks.DataBlock;
import nl.renarj.jasdb.index.keys.Key;
import nl.renarj.jasdb.index.keys.impl.CompositeKey;
import nl.renarj.jasdb.index.keys.keyinfo.KeyLoadResult;
import nl.renarj.jasdb.index.keys.keyinfo.KeyNameMapper;
import nl.renarj.jasdb.index.keys.keyinfo.MultiKeyloader;
import nl.renarj.jasdb.index.keys.types.ComplexKeyType;
import nl.renarj.jasdb.index.keys.types.KeyType;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Set;

/**
 * @author Renze de Vries
 */
public class CompositeKeyFactory implements KeyFactory {
    private MultiKeyloader multiKeyloader;

    public CompositeKeyFactory(MultiKeyloader multiKeyloader) {
        this.multiKeyloader = multiKeyloader;
    }

    @Override
    public Key loadKey(int curPosition, ByteBuffer byteBuffer) throws JasDBStorageException {
        CompositeKey compositeKey = new CompositeKey();
        multiKeyloader.loadKeys(compositeKey, curPosition, byteBuffer);
        return compositeKey;
    }

    @Override
    public void writeKey(Key key, int curPosition, ByteBuffer byteBuffer) throws JasDBStorageException {
        if(key instanceof CompositeKey) {
            multiKeyloader.writeKeys(key, curPosition, byteBuffer);
        } else {
            throw new JasDBStorageException("Unable to write key not a composite key");
        }
    }

    @Override
    public Key createKey(IndexableItem indexableItem) throws JasDBStorageException {
        CompositeKey compositeKey = new CompositeKey();
        this.multiKeyloader.enrichKey(indexableItem, compositeKey);

        return compositeKey;
    }

    @Override
    public Key createEmptyKey() {
        return new CompositeKey();
    }

    @Override
    public KeyLoadResult loadKey(int offset, DataBlock dataBlock) throws JasDBStorageException {
        CompositeKey compositeKey = new CompositeKey();
        KeyLoadResult result = multiKeyloader.loadKeys(compositeKey, offset, dataBlock);

        return new KeyLoadResult(result.getLoadedKey(), result.getEndBlock(), result.getNextOffset());
    }

    @Override
    public DataBlock writeKey(Key key, DataBlock dataBlock) throws JasDBStorageException {
        if(key instanceof CompositeKey) {
            return multiKeyloader.writeKeys(key, dataBlock);
        } else {
            throw new JasDBStorageException("Unable to write key not a composite key");
        }
    }

    @Override
    public Set<Key> createMultivalueKeys(IndexableItem indexableItem) throws JasDBStorageException {
        KeyNameMapper nameMapper = multiKeyloader.getKeyNameMapper();
        Set<Key> currentKeys = new HashSet<>();
        currentKeys.add(new CompositeKey());
        for(KeyFactory keyFactory : multiKeyloader.getKeyFactories()) {
            Set<Key> subKeys = keyFactory.createMultivalueKeys(indexableItem);

            Set<Key> productSet = new HashSet<>();
            for(Key currentKey : currentKeys) {
                for(Key subKey : subKeys) {
                    Key key = currentKey.cloneKey(true);
                    key.addKey(nameMapper, keyFactory.getFieldName(), subKey.cloneKey(false));

                    productSet.add(key);
                }
            }
            currentKeys = productSet;
        }

        return currentKeys;
    }

    @Override
    public boolean isMultiValueKey(IndexableItem indexableItem) throws JasDBStorageException {
        for(KeyFactory keyFactory : multiKeyloader.getKeyFactories()) {
            if(keyFactory.isMultiValueKey(indexableItem)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public Key convertKey(Key key) throws JasDBStorageException {
        if(key instanceof CompositeKey) {
            CompositeKey compositeKey = (CompositeKey) key;
            KeyNameMapper mapper = multiKeyloader.getKeyNameMapper();
            for(KeyFactory keyFactory : multiKeyloader.getKeyFactories()) {
                Key partialKey = compositeKey.getKey(mapper, keyFactory.getFieldName());
                if(partialKey != null && !keyFactory.supportsKey(partialKey)) {
                    Key convertedKey = keyFactory.convertKey(partialKey);
                    compositeKey.addKey(mapper, keyFactory.getFieldName(), convertedKey);
                }
            }

            return key;
        } else {
            CompositeKey compositeKey = new CompositeKey();
            compositeKey.setValueMarker(1);
            compositeKey.setKeys(new Key[]{key});
            return compositeKey;
        }
    }

    public MultiKeyloader getMultiKeyloader() {
        return multiKeyloader;
    }

    @Override
    public boolean supportsKey(Key key) {
        if(key instanceof CompositeKey) {
            CompositeKey compositeKey = (CompositeKey) key;
            KeyNameMapper mapper = multiKeyloader.getKeyNameMapper();
            for(KeyFactory keyFactory : multiKeyloader.getKeyFactories()) {
                Key partialKey = compositeKey.getKey(mapper, keyFactory.getFieldName());
                if(partialKey == null || !keyFactory.supportsKey(partialKey)) {
                    return false;
                }
            }
        }
        return false;
    }

    @Override
    public String asHeader() {
        return "complexType(" + multiKeyloader.asHeader() + ");";
    }

    @Override
    public KeyType getKeyType() {
        return new ComplexKeyType();
    }

    @Override
    public int getKeySize() {
        return multiKeyloader.getKeySize();
    }

    @Override
    public int getMemorySize() {
        return multiKeyloader.getMemorySize();
    }

    @Override
    public String getKeyId() {
        return ComplexKeyType.KEY_ID;
    }

    @Override
    public String getFieldName() {
        StringBuilder nameBuilder = new StringBuilder();
        for(KeyFactory keyFactory : multiKeyloader.getKeyFactories()) {
            nameBuilder.append(keyFactory.getFieldName().replaceAll("_", ""));
        }

        return nameBuilder.toString();
    }
}
