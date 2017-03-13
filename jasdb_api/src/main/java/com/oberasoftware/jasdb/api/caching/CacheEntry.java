package com.oberasoftware.jasdb.api.caching;

import com.oberasoftware.jasdb.api.exceptions.JasDBStorageException;

/**
 * @author Renze de Vries
 */
public interface CacheEntry<X> {
    boolean isInUse();

    long memorySize();

    X getValue();

    void release() throws JasDBStorageException;
}
