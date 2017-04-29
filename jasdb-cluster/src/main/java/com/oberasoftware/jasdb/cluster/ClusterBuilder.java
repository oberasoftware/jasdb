package com.oberasoftware.jasdb.cluster;

import com.oberasoftware.jasdb.cluster.api.ClusterNode;
import com.oberasoftware.jasdb.cluster.copycat.CopyCatClusterNode;
import com.oberasoftware.jasdb.cluster.copycat.CopyCatStateMachine;
import io.atomix.catalyst.transport.Address;
import io.atomix.catalyst.transport.netty.NettyTransport;
import io.atomix.copycat.client.CopycatClient;
import io.atomix.copycat.server.CopycatServer;
import io.atomix.copycat.server.StateMachine;
import io.atomix.copycat.server.storage.Storage;
import io.atomix.copycat.server.storage.StorageLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClusterBuilder {
    private static final Logger LOG = LoggerFactory.getLogger(ClusterBuilder.class);

    private enum JOIN_MODE {
        BOOTSTRAP,
        NODE
    }

    private String bootstrapNode;
    private int bootstrapPort;
    private JOIN_MODE mode;

    private String bindHost;
    private int bindPort;

    private StateMachine stateMachine = new CopyCatStateMachine();

    public static ClusterBuilder create() {
        return new ClusterBuilder();
    }

    public ClusterBuilder bind(String host, int port) {
        this.bindHost = host;
        this.bindPort = port;
        return this;
    }

    public ClusterBuilder asBootstrapNode() {
        this.mode = JOIN_MODE.BOOTSTRAP;
        return this;
    }

    public ClusterBuilder forCluster(String host, int port) {
        this.bootstrapNode = host;
        this.bootstrapPort = port;
        this.mode = JOIN_MODE.NODE;
        return this;
    }

    public ClusterBuilder withStateMachine(StateMachine stateMachine) {
        this.stateMachine = stateMachine;
        return this;
    }

    private CopycatServer createCopyCatServer() {
        return CopycatServer.builder(new Address(bindHost, bindPort))
                .withStorage(new Storage(StorageLevel.MEMORY))
                .withStateMachine(() -> stateMachine)
                .build();
    }

    private CopycatClient createClient() {
        Address address;
        if(mode == JOIN_MODE.NODE) {
            address = new Address(bootstrapNode, bootstrapPort);
        } else {
            address = new Address(bindHost, bindPort);
        }
        return CopycatClient.builder()
                .withTransport(new NettyTransport())
                .build().connect(address).join();
    }

    public ClusterNode build() {
        CopycatServer server = createCopyCatServer();
        if(mode == JOIN_MODE.BOOTSTRAP) {
            server.bootstrap().join();
        } else {
            server.join(new Address(bootstrapNode, bootstrapPort)).join();
        }

        return new CopyCatClusterNode(server, createClient());
    }


}
