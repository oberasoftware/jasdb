package nl.renarj.jasdb.index.keys.keyinfo;

import nl.renarj.jasdb.index.keys.Key;
import nl.renarj.jasdb.index.keys.factory.KeyFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * @author Renze de Vries
 */
public class KeyNameMapperImpl implements KeyNameMapper {
    private Map<String, Integer> fieldIndexes;
    private Map<Integer, String> indexFields;
    private int valueMarker;

    public KeyNameMapperImpl() {
        fieldIndexes = new HashMap<>();
        indexFields = new HashMap<>();
    }

    private KeyNameMapperImpl(Map<String, Integer> fieldIndexes, Map<Integer, String> indexFields) {
        this.fieldIndexes = new HashMap<>(fieldIndexes);
        this.indexFields = new HashMap<>(indexFields);
    }

    public static KeyNameMapperImpl create(KeyFactory[] keyFactories) {
        KeyNameMapperImpl keyNameMapper = new KeyNameMapperImpl();
        for(int i=0; i<keyFactories.length; i++) {
            KeyFactory keyFactory = keyFactories[i];
            keyNameMapper.addMappedField(i, keyFactory.getFieldName());
        }
        return keyNameMapper;
    }

    @Override
    public int getIndexForField(String field) {
        return fieldIndexes.get(field);
    }

    @Override
    public boolean isMapped(String field) {
        return fieldIndexes.containsKey(field);
    }

    @Override
    public int size() {
        return fieldIndexes.size();
    }

    @Override
    public String getFieldForIndex(Integer index) {
        return indexFields.get(index);
    }

    @Override
    public Integer addMappedField(String field) {
        if(!isMapped(field)) {
            int index = fieldIndexes.size();
            addMappedField(index, field);
            return index;
        } else {
            return fieldIndexes.get(field);
        }
    }

    public void addMappedField(Integer index, String field) {
        fieldIndexes.put(field, index);
        indexFields.put(index, field);
    }

    @Override
    public Set<String> getFieldSet() {
        return new HashSet<>(fieldIndexes.keySet());
    }

    @Override
    public boolean isFullyMapped(Key mappedKey) {
        Key[] keys = mappedKey.getKeys();
        if(keys != null && keys.length == fieldIndexes.size()) {
            for(int i=0; i<fieldIndexes.size(); i++) {
                if(keys[i] == null) {
                    return false;
                }
            }
            return true;
        }

        return false;
    }

    @Override
    public int getValueMarker() {
        return valueMarker;
    }

    @Override
    public void setValueMarker(int index) {
        this.valueMarker = index;
    }

    @Override
    public KeyNameMapper clone() {
        return new KeyNameMapperImpl(fieldIndexes, indexFields);
    }

    @Override
    public String toString() {
        return "KeyNameMapperImpl{" +
                "fieldIndexes=" + fieldIndexes +
                '}';
    }
}
