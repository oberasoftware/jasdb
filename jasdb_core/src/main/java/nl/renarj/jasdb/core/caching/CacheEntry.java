package nl.renarj.jasdb.core.caching;

import nl.renarj.jasdb.core.exceptions.JasDBStorageException;

/**
 * @author Renze de Vries
 */
public interface CacheEntry<X> {
    boolean isInUse();

    long memorySize();

    X getValue();

    void release() throws JasDBStorageException;
}
