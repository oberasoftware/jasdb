package nl.renarj.jasdb.index.keys.impl;

import nl.renarj.jasdb.index.keys.AbstractKey;
import nl.renarj.jasdb.index.keys.CompareMethod;
import nl.renarj.jasdb.index.keys.CompareResult;
import nl.renarj.jasdb.index.keys.Key;
import nl.renarj.jasdb.index.keys.keyinfo.KeyNameMapper;

import java.util.Arrays;
import java.util.Map;

/**
 * @author Renze de Vries
 */
public class CompositeKey extends AbstractKey {
    private int valueMarker = 0;

    @Override
    public boolean equals(Object obj) {
        return obj instanceof CompositeKey && compare(((CompositeKey) obj).getKeys(), CompareMethod.EQUALS).getCompare() == 0;
    }

    @Override
    public Key addKey(KeyNameMapper keyMapper, String name, Key key) {
        this.valueMarker = keyMapper.getValueMarker();
        return super.addKey(keyMapper, name, key);
    }

    @Override
    public Key setKeys(KeyNameMapper keyMapper, Map<String, Key> keyFields) {
        this.valueMarker = keyMapper.getValueMarker();
        return super.setKeys(keyMapper, keyFields);
    }

    @Override
    public Key cloneKey() {
        return cloneKey(true);
    }

    public void setValueMarker(int valueMarker) {
        this.valueMarker = valueMarker;
    }

    @Override
    public Key cloneKey(boolean includeChildren) {
        CompositeKey clonedKey = new CompositeKey();
        clonedKey.setKeys(getKeys());
        clonedKey.setValueMarker(valueMarker);

        return clonedKey;
    }

    private CompareResult compare(Key[] otherKeys, CompareMethod compareMethod) {
        Key[] keys = getKeys();
        //we assume keys are ordered in same order
        int keyLength = keys.length;
        int lastCompare = -1;
        for(int i=0; i<keyLength; i++) {
            Key key = keys[i];
            Key otherKey = otherKeys.length > i ? otherKeys[i] : null;
            if(key != null && otherKey != null && i<valueMarker) {
                lastCompare = key.compareTo(otherKey);
                if(lastCompare != 0) {
                    return new CompareResult(lastCompare);
                }
            } else {
                break;
            }
        }

        if(compareMethod == CompareMethod.BEFORE) {
            return new CompareResult(-1);
        } else {
            return new CompareResult(lastCompare);
        }
    }

    @Override
    public CompareResult compare(Key otherKey, CompareMethod method) {
        if(otherKey instanceof CompositeKey) {
            return compare((otherKey).getKeys(), method);
        } else {
            return compare(new Key[] {otherKey}, method);
        }
    }

    @Override
    public CompositeKey getValue() {
        return this;
    }

    @Override
    public int hashCode() {
        return getKeys() != null ? Arrays.hashCode(getKeys()) : 0;
    }

    @Override
    public String toString() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("CompositeKey[");
        for(Key key : getKeys()) {
            stringBuilder.append(key).append(";");
        }
        stringBuilder.append("]");
        return stringBuilder.toString();
    }
}
