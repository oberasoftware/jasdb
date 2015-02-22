/*
 * The JASDB software and code is Copyright protected 2011 and owned by Renze de Vries
 * 
 * All the code and design principals in the codebase are also Copyright 2011 
 * protected and owned Renze de Vries. Any unauthorized usage of the code or the 
 * design and principals as in this code is prohibited.
 */
package nl.renarj.jasdb.index.keys.impl;

import nl.renarj.jasdb.core.MEMORY_CONSTANTS;
import nl.renarj.jasdb.core.exceptions.RuntimeJasDBException;
import nl.renarj.jasdb.index.keys.AbstractKey;
import nl.renarj.jasdb.index.keys.CompareMethod;
import nl.renarj.jasdb.index.keys.CompareResult;
import nl.renarj.jasdb.index.keys.Key;

import java.util.Arrays;

import static nl.renarj.core.utilities.conversion.LongUtils.bytesToLong;
import static nl.renarj.core.utilities.conversion.LongUtils.longToBytes;

public class LongKey extends AbstractKey {
	//private long key;
    private final byte[] key;
	
	public LongKey(long key) {
		this.key = longToBytes(key);
	}

    public LongKey(byte[] bytes) {
        this.key = bytes;
    }

	public long getKey() {
		return bytesToLong(key);
	}
	
    @Override
    public String toString() {
        return "LongKey{" +
                "key=" + getKey() +
                '}';
    }

    @Override
    public CompareResult compare(Key o, CompareMethod method) {
        if(o != null) {
            if(o instanceof CompositeKey) {
                CompositeKey localKey = new CompositeKey();
                localKey.setKeys(new Key[] {this});
                localKey.setValueMarker(1);

                return localKey.compare(o, method);
            } else {
                return evaluateKey(o);
            }
        } else {
            throw new RuntimeJasDBException("Cannot compare null Key");
        }
    }

    private CompareResult evaluateKey(Key o) {
        int result;
        if(o instanceof LongKey) {
            LongKey longIndexPointer = (LongKey) o;

            if(key.length == 0) {
                result = 0;
            } else {
                result = (getKey() < longIndexPointer.getKey()) ? -1 : ((getKey() == longIndexPointer.getKey()) ? 0 : 1);
            }
        } else if(o instanceof  StringKey) {
            StringKey stringKey = (StringKey) o;
            try {
                result = Long.valueOf(getKey()).compareTo(Long.parseLong(stringKey.getKey()));
            } catch(NumberFormatException ignored) {
                result = -1;
            }
        } else {
            result = -1;
        }

        return new CompareResult(result);
    }

    @Override
    public long size() {
        return MEMORY_CONSTANTS.LONG_BYTE_SIZE + super.size();
    }

    @Override
    public int getKeyCount() {
        return super.getKeyCount() + 1;
    }

    @Override
	public int hashCode() {
		return Arrays.hashCode(key);
	}

	@Override
	public boolean equals(Object obj) {
		if(obj instanceof LongKey) {
			LongKey longIndexPointer = (LongKey) obj;

			return longIndexPointer.getKey() == getKey();
		} else {
			return false;
		}
	}
	
	@Override
	public Key cloneKey() {
		return cloneKey(true);
	}

	@Override
	public Key cloneKey(boolean includeValues) {
		Key stringKey = new LongKey(key);
		return includeValues ? stringKey.setKeys(getKeys()) : stringKey;
	}

    @Override
    public Long getValue() {
        return getKey();
    }
}
