package com.oberasoftware.jasdb.cluster.test;

import com.oberasoftware.jasdb.cluster.ClusterBuilder;
import com.oberasoftware.jasdb.cluster.api.ClusterClient;
import com.oberasoftware.jasdb.cluster.api.ClusterNode;
import com.oberasoftware.jasdb.cluster.api.DistributedMap;
import com.oberasoftware.jasdb.cluster.copycat.CopyCatStateMachine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReplicaNode {
    private static final Logger LOG = LoggerFactory.getLogger(ReplicaNode.class);

    public static void main(String[] args) {
        LOG.info("Starting a replica node");

        ClusterNode node = ClusterBuilder.create()
                .bind("127.0.0.1", 5001)
                .forCluster("127.0.0.1", 5000)
                .withStateMachine(new CopyCatStateMachine())
                .build();
        LOG.info("Replica node started");

        LOG.info("Adding data");
        ClusterClient clusterClient = node.getClusterClient();
        DistributedMap<String, String> productMap = clusterClient.getMap("productMap");
        productMap.put("car", "tesla");
        productMap.put("laptop", "macbook");


//        DistributedMap<String, Integer> counterMap = clusterClient.getMap("counters");
//        counterMap.put("legobricks", 100000);
//        counterMap.put("ownedrobots", 3);
//        LOG.info("Data inserted");
    }
}
