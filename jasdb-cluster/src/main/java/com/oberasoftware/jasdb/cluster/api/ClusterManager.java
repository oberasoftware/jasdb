package com.oberasoftware.jasdb.cluster.api;

import com.oberasoftware.jasdb.cluster.model.Partition;

import java.util.List;

/**
 * @author renarj
 */
public interface ClusterManager {
    boolean join();

    List<Partition> getPartitions(String instanceId, String bag);
}
