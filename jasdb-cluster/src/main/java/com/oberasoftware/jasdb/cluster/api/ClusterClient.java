package com.oberasoftware.jasdb.cluster.api;

public interface ClusterClient {
    <K, V> DistributedMap<K, V> getMap(String mapName);

    DistributedLock getLock(String lockName);
}
