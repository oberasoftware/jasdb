package com.oberasoftware.jasdb.cluster;

import com.oberasoftware.jasdb.api.engine.Configuration;
import com.oberasoftware.jasdb.api.engine.ConfigurationLoader;
import com.oberasoftware.jasdb.api.exceptions.ConfigurationException;
import jakarta.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author Renze de Vries
 */
@Component
public class ClusterConfiguration {

    private static final int DEFAULT_REPLICATION_FACTOR = 1;
    private static final int DEFAULT_SHARDS = 32;
    private Map<String, Configuration> configPropertyMap = new HashMap<>();

    private static final int DEFAULT_PORT = 5000;
    private static final String MASTER_HOST = "127.0.0.1";

    private final ConfigurationLoader configurationLoader;

    private boolean master;

    private int clusterMasterPort;
    private String clusterMasterHost;

    private int bindPort;
    private String bindAddress;

    private List<PartitionConfig> partitionConfigs;

    @Autowired
    public ClusterConfiguration(ConfigurationLoader configurationLoader) {
        this.configurationLoader = configurationLoader;
    }

    @PostConstruct
    public void configure() throws ConfigurationException {
        Configuration configuration = configurationLoader.getConfiguration();
        Configuration clusterConfig = configuration.getChildConfiguration("/jasdb/Cluster");

        master = clusterConfig.getAttribute("master", false);
        bindPort = clusterConfig.getAttribute("NodePort", DEFAULT_PORT);
        bindAddress = clusterConfig.getAttribute("Bind", MASTER_HOST);

        if(!master) {
            Configuration masterConfig = clusterConfig.getChildConfiguration("Master");
            clusterMasterPort = masterConfig.getAttribute("Port", DEFAULT_PORT);
            clusterMasterHost = masterConfig.getAttribute("Host", MASTER_HOST);
        }

        clusterConfig.getChildConfigurations("Property").forEach(p -> {
            configPropertyMap.put(p.getAttribute("Name"), p);
        });

        partitionConfigs = new ArrayList<>();
        clusterConfig.getChildConfigurations("Partition").forEach(p -> {
            String bag = p.getAttribute("Bag");
            String instance = p.getAttribute("Instance");

            List<String> fields = p.getChildConfigurations("Field").stream()
                    .map(f -> f.getAttribute("Name"))
                    .collect(Collectors.toList());
            partitionConfigs.add(new PartitionConfig(instance, bag, fields));
        });
    }

    List<PartitionConfig> getPartitionConfigurations() {
        return partitionConfigs;
    }

    int getClusterMasterPort() {
        return clusterMasterPort;
    }

    String getClusterMasterHost() {
        return clusterMasterHost;
    }

    int getBindPort() {
        return bindPort;
    }

    String getBindAddress() {
        return bindAddress;
    }

    boolean isMaster() {
        return master;
    }

    int getReplicationFactor() {
        return getProperty("replicationFactor", DEFAULT_REPLICATION_FACTOR);
    }

    int getBootstrapShards() {
        return getProperty("bootstrapShards", DEFAULT_SHARDS);
    }

    private int getProperty(String property, int defaultValue) {
        if(configPropertyMap.containsKey(property)) {
            return configPropertyMap.get(property).getAttribute("Value", defaultValue);
        }
        return defaultValue;
    }
}
