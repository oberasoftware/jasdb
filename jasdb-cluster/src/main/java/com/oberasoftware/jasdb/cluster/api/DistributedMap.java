package com.oberasoftware.jasdb.cluster.api;

import java.util.Collection;

public interface DistributedMap<K, V> {
    void put(K k, V v);

    void remove(K k);

    Collection<V> values();

    V get(K k);

    boolean isEmpty();

    int size();
}
