package com.oberasoftware.jasdb.api.caching;

import java.util.Collection;

/**
 * @author Renze de Vries
 */
public interface CacheRegion<T extends Comparable<T>, X extends CacheEntry> {
    String name();

    long lastRegionAccess();

    long memorySize();

    long reduceBy(long reduceSize);

    X putEntry(T key, X entry);

    boolean contains(T key);

    X getEntry(T key);

    Collection<X> values();

    int size();

    boolean removeEntry(T key);

    void clear();
}
