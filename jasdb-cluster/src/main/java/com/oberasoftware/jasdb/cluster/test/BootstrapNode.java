package com.oberasoftware.jasdb.cluster.test;

import com.google.common.util.concurrent.Uninterruptibles;
import com.oberasoftware.jasdb.cluster.ClusterBuilder;
import com.oberasoftware.jasdb.cluster.api.ClusterNode;
import com.oberasoftware.jasdb.cluster.api.DistributedMap;
import com.oberasoftware.jasdb.cluster.copycat.CopyCatStateMachine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.concurrent.TimeUnit;

public class BootstrapNode {
    private static final Logger LOG = LoggerFactory.getLogger(BootstrapNode.class);

    public static void main(String[] args) {
        LOG.info("Starting bootstrap node");

        ClusterNode node = ClusterBuilder.create()
                .bind("127.0.0.1", 5000)
                .asBootstrapNode()
                .withStateMachine(new CopyCatStateMachine())
                .build();
        LOG.info("Cluster bootstrap node started");

        Uninterruptibles.sleepUninterruptibly(10, TimeUnit.SECONDS);
        DistributedMap<String, String> map = node.getClusterClient().getMap("productMap");
        LOG.info("Car: {}", map.get("car"));
        LOG.info("laptop: {}", map.get("laptop"));

        Collection<String> values = map.values();
        values.forEach(v -> LOG.info("Value: {}", v));
    }
}
