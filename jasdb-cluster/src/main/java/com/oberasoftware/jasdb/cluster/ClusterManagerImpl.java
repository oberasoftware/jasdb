package com.oberasoftware.jasdb.cluster;

import com.oberasoftware.jasdb.cluster.api.ClusterClient;
import com.oberasoftware.jasdb.cluster.api.ClusterNode;
import com.oberasoftware.jasdb.cluster.api.ClusterManager;
import com.oberasoftware.jasdb.cluster.copycat.CopyCatStateMachine;
import com.oberasoftware.jasdb.cluster.model.Partition;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.stream.Collectors;

import static org.slf4j.LoggerFactory.getLogger;

/**
 * @author Renze de Vries
 */
@Component
public class ClusterManagerImpl implements ClusterManager {
    private static final Logger LOG = getLogger(ClusterManagerImpl.class);

    private final ClusterConfiguration gridConfiguration;

    private ClusterNode node;
    private ClusterClient clusterClient;

    private final List<JoinProcess> clusterProcesses;

    @Autowired
    public ClusterManagerImpl(ClusterConfiguration gridConfiguration, List<JoinProcess> clusterProcesses) {
        this.gridConfiguration = gridConfiguration;
        this.clusterProcesses = clusterProcesses;
    }

    @Override
    public boolean join() {
        connectToCluster();

        List<JoinProcess> jps = clusterProcesses.stream()
                .filter(cp -> cp.shouldRun(clusterClient)).collect(Collectors.toList());
        for (JoinProcess jp : jps) {
            LOG.info("Running cluster join process: {}", jp);
            if(!jp.run(clusterClient)) {
                LOG.error("Could not complete cluster join process: {} stopping system", jp);
                System.exit(-1);
            }
        }


        return this.node != null;
    }

    private void connectToCluster() {
        String bindAddress = gridConfiguration.getBindAddress();
        int bindPort = gridConfiguration.getBindPort();

        LOG.info("Creating cluster agent binding to: {}:{}", bindAddress, bindPort);
        ClusterBuilder builder = ClusterBuilder.create().bind(bindAddress, bindPort);
        if(gridConfiguration.isMaster()) {
            LOG.info("Creating bootstrap node");
            builder.asBootstrapNode();
        } else {
            String clusterHost = gridConfiguration.getClusterMasterHost();
            int clusterPort = gridConfiguration.getClusterMasterPort();
            LOG.info("Connecting to master: {}:{}", clusterHost, clusterPort);
            builder.forCluster(clusterHost, clusterPort);
        }

        this.node = builder
                .withStateMachine(new CopyCatStateMachine())
                .build();
        this.clusterClient = this.node.getClusterClient();
    }

    @Override
    public List<Partition> getPartitions(String instanceId, String bag) {
        return null;
    }
}
