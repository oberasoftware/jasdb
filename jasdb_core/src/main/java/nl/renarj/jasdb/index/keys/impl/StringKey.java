/*
 * The JASDB software and code is Copyright protected 2011 and owned by Renze de Vries
 * 
 * All the code and design principals in the codebase are also Copyright 2011 
 * protected and owned Renze de Vries. Any unauthorized usage of the code or the 
 * design and principals as in this code is prohibited.
 */
package nl.renarj.jasdb.index.keys.impl;

import nl.renarj.jasdb.core.exceptions.RuntimeJasDBException;
import nl.renarj.jasdb.index.keys.AbstractKey;
import nl.renarj.jasdb.index.keys.CompareMethod;
import nl.renarj.jasdb.index.keys.CompareResult;
import nl.renarj.jasdb.index.keys.Key;
import nl.renarj.jasdb.index.keys.KeyUtil;

import java.util.Arrays;
import java.util.UUID;

public class StringKey extends AbstractKey {
    private byte[] unicodeBytes;
	
	public StringKey(String stringIndex) {
        this.unicodeBytes = KeyUtil.getUnicodeBytes(stringIndex.trim().toLowerCase());
	}

    public StringKey(byte[] unicodeBytes) {
        int lastNull = unicodeBytes.length;
        for(int i=(unicodeBytes.length - 1); i>=0; i--) {
            if(unicodeBytes[i] == 0) {
                lastNull = i;
            } else {
                break;
            }

        }
        if(lastNull > 0) {
            this.unicodeBytes = Arrays.copyOf(unicodeBytes, lastNull);
        } else {
            this.unicodeBytes = new byte[0];
        }
    }
	
	public String getKey() {
		return KeyUtil.getUnicodeString(unicodeBytes);
	}

    public byte[] getUnicodeBytes() {
        return this.unicodeBytes;
    }

    @Override
    public long size() {
        return unicodeBytes.length + super.size();
    }

    @Override
    public int getKeyCount() {
        return super.getKeyCount() + 1;
    }

    @Override
    public CompareResult compare(Key o, CompareMethod method) {
        if(o != null) {
            int result = -1;
            if(o instanceof StringKey) {
                result = compare(unicodeBytes, ((StringKey)o).getUnicodeBytes());
            } else if(o instanceof LongKey) {
                LongKey longKey = (LongKey) o;

                try {
                    LongKey localKey = new LongKey(Long.parseLong(getKey()));
                    result = localKey.compareTo(longKey);
                } catch(NumberFormatException e) {
                    throw new RuntimeJasDBException("Unable to compare long key: " + o.toString() + " to local string key: " + getKey());
                }
            } else if(o instanceof  UUIDKey) {
                UUIDKey uuidKey = (UUIDKey) o;
                byte[] uuidBytes = KeyUtil.getUnicodeBytes(new UUID(uuidKey.getMostSignificant(), uuidKey.getLeastSignificant()).toString());

                result = compare(unicodeBytes, uuidBytes);
            }

            return new CompareResult(result);
        } else {
            throw new RuntimeJasDBException("Cannot compare null Key");
        }    }

    private int compare(byte[] left, byte[] right) {
        for (int i = 0, j = 0; i < left.length && j < right.length; i++, j++) {
            int a = (left[i] & 0xff);
            int b = (right[j] & 0xff);
            if (a != b) {
                return a - b;
            }
        }
        return left.length - right.length;
    }

    @Override
	public int hashCode() {
        return Arrays.hashCode(unicodeBytes);
	}

	@Override
	public boolean equals(Object obj) {
		if(obj instanceof StringKey) {
			StringKey indexPointer = (StringKey) obj;
            return indexPointer.compareTo(this) == 0;
		} else {
			return false;
		}
	}
	
    @Override
    public String toString() {
        return "StringKey{" +
                "key=" + getKey() +
                '}';
    }

    @Override
	public Key cloneKey() {
		return cloneKey(true);
	}

	@Override
	public Key cloneKey(boolean includeValues) {
		Key stringKey = new StringKey(unicodeBytes);
		return includeValues ? stringKey.setKeys(getKeys()) : stringKey;
	}

    @Override
    public String getValue() {
        return getKey();
    }
}
