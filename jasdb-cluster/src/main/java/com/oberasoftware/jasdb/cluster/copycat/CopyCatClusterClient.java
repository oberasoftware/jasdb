package com.oberasoftware.jasdb.cluster.copycat;

import com.oberasoftware.jasdb.cluster.api.ClusterClient;
import com.oberasoftware.jasdb.cluster.api.DistributedLock;
import com.oberasoftware.jasdb.cluster.api.DistributedMap;
import com.oberasoftware.jasdb.cluster.copycat.lock.LockClientImpl;
import com.oberasoftware.jasdb.cluster.copycat.map.MapClientImpl;
import io.atomix.copycat.client.CopycatClient;

public class CopyCatClusterClient implements ClusterClient {
    private CopycatClient copycatClient;

    public CopyCatClusterClient(CopycatClient copycatClient) {
        this.copycatClient = copycatClient;
    }

    @Override
    public <K, V> DistributedMap<K, V> getMap(String mapName) {
        return new MapClientImpl<>(mapName, copycatClient);
    }

    @Override
    public DistributedLock getLock(String lockName) {
        return new LockClientImpl(lockName, copycatClient);
    }
}
