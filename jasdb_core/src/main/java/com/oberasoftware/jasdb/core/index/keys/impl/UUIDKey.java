/*
 * The JASDB software and code is Copyright protected 2011 and owned by Renze de Vries
 * 
 * All the code and design principals in the codebase are also Copyright 2011 
 * protected and owned Renze de Vries. Any unauthorized usage of the code or the 
 * design and principals as in this code is prohibited.
 */
package nl.renarj.jasdb.index.keys.impl;

import com.oberasoftware.jasdb.core.MEMORY_CONSTANTS;
import nl.renarj.jasdb.index.keys.AbstractKey;
import nl.renarj.jasdb.index.keys.CompareMethod;
import nl.renarj.jasdb.index.keys.CompareResult;
import nl.renarj.jasdb.index.keys.Key;

import java.util.UUID;

/**
 * This represents the UUID key class mainly used for object references, this
 * relies on the UUID implementation of Java (java.util.UUID) but does tries
 * to prevent usage of the object itself for memory reasons. Therefore some of the
 * compare and hashcode have been copied to directly work on the least and most
 * significant long values contained in this class.
 *
 * @author Renze de Vries
 */
public class UUIDKey extends AbstractKey {
    private long leastSignificant;
    private long mostSignificant;
	
	public UUIDKey(String id) {
        UUID uuid = UUID.fromString(id);
        this.leastSignificant = uuid.getLeastSignificantBits();
        this.mostSignificant = uuid.getMostSignificantBits();
	}
	
	public UUIDKey(UUID uuid) {
        this.leastSignificant = uuid.getLeastSignificantBits();
        this.mostSignificant = uuid.getMostSignificantBits();
	}

    public UUIDKey(long leastSignificant, long mostSignificant) {
        this.leastSignificant = leastSignificant;
        this.mostSignificant = mostSignificant;
    }

    public long getLeastSignificant() {
        return leastSignificant;
    }

    public long getMostSignificant() {
        return mostSignificant;
    }

    @Override
    public long size() {
        return MEMORY_CONSTANTS.TWO_LONG_BYTES + super.size();
    }

    @Override
    public int getKeyCount() {
        return super.getKeyCount() + 1;
    }

    @Override
    public CompareResult compare(Key o, CompareMethod method) {
        if(o != null) {
            if(o instanceof UUIDKey) {
                UUIDKey otherKey = (UUIDKey) o;
                return new CompareResult(compare(otherKey));
            } else {
                return new CompareResult(-1);
            }
        } else {
            throw new NullPointerException("Cannot compare null Key");
        }
    }

    /**
     * Compare method implementation simple value compare, just like the java.util.UUID also handles this
     * @param otherKey The other UUIDKey to compare to
     * @return the compare result -1 if less, 0 if equal and 1 if larger
     */
    private int compare(UUIDKey otherKey) {
        return (
                this.mostSignificant < otherKey.mostSignificant ? -1 : (
                        this.mostSignificant > otherKey.mostSignificant ? 1 : (
                                this.leastSignificant < otherKey.leastSignificant ? -1 : (
                                        this.leastSignificant > otherKey.leastSignificant ? 1 : 0
                                )
                        )
                )
        );
    }
	
	@Override
	public int hashCode() {
        //hashcode copied from java.util.UUID as we should have similar behaviour as that class
        long hilo = mostSignificant ^ leastSignificant;
        return ((int)(hilo >> 32)) ^ (int) hilo;

    }

	@Override
	public boolean equals(Object obj) {
		if(obj instanceof UUIDKey) {
			UUIDKey uuidPointer = (UUIDKey) obj;
            return uuidPointer.compareTo(this) == 0;
		} else {
			return false;
		}
	}

    @Override
    public String toString() {
        return "UUIDKey{" +
                "uuid=" + new UUID(mostSignificant, leastSignificant).toString() +
                ", leastSignificant=" + leastSignificant +
                ", mostSignificant=" + mostSignificant +
                '}';
    }

    @Override
	public Key cloneKey() {
		return cloneKey(true);
	}

	@Override
	public Key cloneKey(boolean includeValues) {
		Key stringKey = new UUIDKey(leastSignificant, mostSignificant);
		return includeValues ? stringKey.setKeys(getKeys()) : stringKey;
	}

    @Override
    public String getValue() {
        return new UUID(mostSignificant, leastSignificant).toString();
    }
}
