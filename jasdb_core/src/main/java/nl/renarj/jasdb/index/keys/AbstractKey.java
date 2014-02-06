/*
 * The JASDB software and code is Copyright protected 2011 and owned by Renze de Vries
 * 
 * All the code and design principals in the codebase are also Copyright 2011 
 * protected and owned Renze de Vries. Any unauthorized usage of the code or the 
 * design and principals as in this code is prohibited.
 */
package nl.renarj.jasdb.index.keys;

import nl.renarj.jasdb.core.MEMORY_CONSTANTS;
import nl.renarj.jasdb.index.keys.impl.CompositeKey;
import nl.renarj.jasdb.index.keys.keyinfo.KeyNameMapper;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public abstract class AbstractKey implements Key {
    private Key[] keys;

	protected AbstractKey() {

	}

    @Override
    public Key setKeys(KeyNameMapper keyMapper, Map<String, Key> keyFields) {
        this.keys = new Key[keyMapper.size()];
        for(Map.Entry<String, Key> keyEntry : keyFields.entrySet()) {
            keys[keyMapper.getIndexForField(keyEntry.getKey())] = keyEntry.getValue();
        }
        return this;
    }

    @Override
    public Key setKeys(Key[] keys) {
        this.keys = keys;
        return this;
    }

    @Override
    public Key addKey(KeyNameMapper keyMapper, String name, Key key) {
        int index = keyMapper.getIndexForField(name);
        int size = keyMapper.size();
        if(keys == null) {
            keys = new Key[size];
        } else if(index < size) {
            keys = Arrays.copyOf(keys, size);
        }

        keys[index] = key;

        return this;
    }

    @Override
    public Key getKey(int index) {
        return keys[index];
    }

    @Override
    public Key getKey(KeyNameMapper keyMapper, String name) {
        return getKey(keyMapper.getIndexForField(name));
    }

    @Override
    public boolean hasChildren() {
        return keys != null;
    }

    @Override
    public Key[] getKeys() {
        return keys;
    }

    @Override
    public long size() {
        long size = MEMORY_CONSTANTS.OBJECT_SIZE;
        if(keys != null) {
            size += MEMORY_CONSTANTS.ARRAY_SIZE + MEMORY_CONSTANTS.OBJECT_SIZE;
            for(Key key : keys) {
                size += key.size() + MEMORY_CONSTANTS.OBJECT_REF;
            }
        }
        return size;
    }

    @Override
    public int getKeyCount() {
        return keys != null ? keys.length : 0;
    }

    @Override
    public CompositeKey getChildrenAsComposite() {
        CompositeKey compositeKey = new CompositeKey();
        compositeKey.setKeys(keys);
        compositeKey.setValueMarker(keys.length);
        return compositeKey;
    }

    @Override
    public Map<String, Key> getKeysByName(KeyNameMapper keyMapper) {
        Map<String, Key> mappedKeys = new HashMap<>();
        for(int i=0; i<keys.length; i++) {
            mappedKeys.put(keyMapper.getFieldForIndex(i), keys[i]);
        }
        return mappedKeys;
    }

    @Override
    public int compareTo(Key o) {
        return compare(o, CompareMethod.EQUALS).getCompare();
    }
}
