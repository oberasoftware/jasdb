package com.oberasoftware.jasdb.cluster.copycat;

import com.oberasoftware.jasdb.cluster.api.ClusterClient;
import com.oberasoftware.jasdb.cluster.api.ClusterNode;
import io.atomix.copycat.client.CopycatClient;
import io.atomix.copycat.server.CopycatServer;

public class CopyCatClusterNode implements ClusterNode {
    private CopycatServer copycatServer;
    private CopycatClient copycatClient;

    public CopyCatClusterNode(CopycatServer copycatServer, CopycatClient copycatClient) {
        this.copycatServer = copycatServer;
        this.copycatClient = copycatClient;
    }

    @Override
    public ClusterClient getClusterClient() {
        return new CopyCatClusterClient(copycatClient);
    }
}
