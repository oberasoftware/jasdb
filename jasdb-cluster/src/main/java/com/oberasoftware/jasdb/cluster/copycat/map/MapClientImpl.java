package com.oberasoftware.jasdb.cluster.copycat.map;

import com.oberasoftware.jasdb.cluster.api.DistributedMap;
import io.atomix.copycat.client.CopycatClient;
import org.slf4j.Logger;

import java.util.Collection;
import java.util.stream.Collectors;

import static org.slf4j.LoggerFactory.getLogger;

public class MapClientImpl<K, V> implements DistributedMap<K, V> {
    private static final Logger LOG = getLogger(MapClientImpl.class);

    private CopycatClient copycatClient;
    private String mapName;

    public MapClientImpl(String mapName, CopycatClient copycatClient) {
        this.copycatClient = copycatClient;
        this.mapName = mapName;
    }

    @Override
    public void put(K k, V v) {
        copycatClient.submit(new PutCommand(mapName, k, v)).join();
    }

    @Override
    public void remove(K k) {
        copycatClient.submit(new RemoveCommand(mapName, k));
    }

    @Override
    public Collection<V> values() {
        Collection<Object> c = copycatClient.submit(new GetValuesCommand(mapName)).join();
        return c.stream().map(o -> (V)o).collect(Collectors.toList());
    }

    @Override
    public boolean isEmpty() {
        return size() == 0;
    }

    @Override
    public int size() {
        Integer n = copycatClient.submit(new GetSizeCommand(mapName)).join();
        LOG.info("Size: {}", n);
        return n;
    }

    @Override
    public V get(K k) {
        return (V)copycatClient.submit(new GetCommand(mapName, k)).join();
    }
}
