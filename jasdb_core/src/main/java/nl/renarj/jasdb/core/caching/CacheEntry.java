package nl.renarj.jasdb.core.caching;

/**
 * @author Renze de Vries
 */
public interface CacheEntry<X> {
    boolean isInUse();

    long memorySize();

    X getValue();
}
