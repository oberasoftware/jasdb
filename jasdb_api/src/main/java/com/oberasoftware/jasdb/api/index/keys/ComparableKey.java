package com.oberasoftware.jasdb.api.index.keys;

/**
 * @author Renze de Vries
 */
public interface ComparableKey<T extends Key> extends Comparable<T> {
    CompareResult compare(T otherKey, CompareMethod method);
}
