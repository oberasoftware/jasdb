package com.oberasoftware.jasdb.cluster;

import com.oberasoftware.jasdb.api.engine.EngineManager;
import com.oberasoftware.jasdb.api.model.NodeInformation;
import com.oberasoftware.jasdb.cluster.api.ClusterClient;
import com.oberasoftware.jasdb.cluster.api.DistributedLock;
import com.oberasoftware.jasdb.cluster.api.DistributedMap;
import com.oberasoftware.jasdb.cluster.model.HexRange;
import com.oberasoftware.jasdb.cluster.model.Partition;
import com.oberasoftware.jasdb.cluster.model.PartitionStatus;
import com.oberasoftware.jasdb.cluster.model.PartitionType;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * @author Renze de Vries
 */
@Component
public class BootstrapClusterProcess implements JoinProcess {
    private static final Logger LOG = getLogger(BootstrapClusterProcess.class);

    private static final String PARTITION_KEY_FORMAT = "%s.%s.%s-%s";

    private final ClusterConfiguration gridConfiguration;

    private final EngineManager engineManager;

    @Autowired
    public BootstrapClusterProcess(ClusterConfiguration gridConfiguration, EngineManager engineManager) {
        this.gridConfiguration = gridConfiguration;
        this.engineManager = engineManager;
    }

    @Override
    public boolean shouldRun(ClusterClient client) {
        LOG.info("Acquiring bootstrap lock");
        DistributedLock lock = client.getLock("bootstrap");
        lock.lock();
        try {
            boolean partitionsEmpty = client.getMap("partitions").isEmpty();
            LOG.info("Checking existing partition present: {}", !partitionsEmpty);
            return partitionsEmpty;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean run(ClusterClient client) {
        DistributedLock lock = client.getLock("bootstrap");
        lock.lock();
        try {
            LOG.info("Running bootstrap empty cluster process");

            DistributedMap<String, Partition> partitionMap = client.getMap("partitions");
            if(partitionMap.isEmpty()) {
                int desiredShards = gridConfiguration.getBootstrapShards();
                List<HexRange> ranges = HexRange.generate(desiredShards);
                NodeInformation nodeInformation = engineManager.getNodeInformation();

                gridConfiguration.getPartitionConfigurations().forEach(pc -> {
                    List<Partition> partitions = ranges.stream().map(r -> {
                        Partition partition = new Partition(pc.getInstanceId(), pc.getBag());
                        partition.setFields(pc.getFields());
                        partition.setNodeInformation(nodeInformation);
                        partition.setStatus(PartitionStatus.ACTIVE);
                        partition.setType(PartitionType.PRIMARY);
                        partition.setPartitionKey(format(PARTITION_KEY_FORMAT, pc.getInstanceId(), pc.getBag(), r.getStart(), r.getEnd()));

                        return partition;
                    }).collect(Collectors.toList());

                    partitions.forEach(p -> {
                        LOG.info("Created partition: {}", p);
                        partitionMap.put(p.getPartitionKey(), p);
                    });
                });
            }
        } finally {
            lock.unlock();
        }

        return true;
    }


}
