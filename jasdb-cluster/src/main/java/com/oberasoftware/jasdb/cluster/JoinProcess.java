package com.oberasoftware.jasdb.cluster;

import com.oberasoftware.jasdb.cluster.api.ClusterClient;

/**
 * @author renarj
 */
public interface JoinProcess {
    boolean shouldRun(ClusterClient client);

    boolean run(ClusterClient client);
}
