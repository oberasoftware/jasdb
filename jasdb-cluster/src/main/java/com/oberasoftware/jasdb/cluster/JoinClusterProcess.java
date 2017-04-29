package com.oberasoftware.jasdb.cluster;

import com.oberasoftware.jasdb.api.engine.EngineManager;
import com.oberasoftware.jasdb.api.model.NodeInformation;
import com.oberasoftware.jasdb.cluster.api.ClusterClient;
import com.oberasoftware.jasdb.cluster.api.DistributedLock;
import com.oberasoftware.jasdb.cluster.api.DistributedMap;
import com.oberasoftware.jasdb.cluster.model.Partition;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import static org.slf4j.LoggerFactory.getLogger;

/**
 * @author renarj
 */
@Component
public class JoinClusterProcess implements JoinProcess {
    private static final Logger LOG = getLogger(JoinClusterProcess.class);

    @Autowired
    private EngineManager engineManager;

    @Override
    public boolean shouldRun(ClusterClient client) {
        LOG.info("Acquiring join cluster lock");
        DistributedLock lock = client.getLock("join");
        lock.lock();
        try {
            DistributedMap<String, Partition> partitions = client.getMap("partitions");
            if(!partitions.isEmpty()) {
                NodeInformation nodeInformation = engineManager.getNodeInformation();

//                partitions.values().stream().filter(p -> {
//                    p.getNodeInformation().
//                });

            }

            boolean partitionsEmpty = client.getMap("partitions").isEmpty();
        } finally {
            lock.unlock();
        }

        return false;
    }

    @Override
    public boolean run(ClusterClient client) {
        return false;
    }
}
