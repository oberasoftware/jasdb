package com.oberasoftware.jasdb.cluster;

import com.oberasoftware.jasdb.cluster.api.ClusterManager;
import com.oberasoftware.jasdb.engine.EngineConfiguation;
import com.oberasoftware.jasdb.rest.service.RestConfiguration;
import com.oberasoftware.jasdb.service.ServiceConfiguration;
import org.slf4j.Logger;
import org.springframework.boot.Banner;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Import;

import java.util.concurrent.ExecutionException;

import static org.slf4j.LoggerFactory.getLogger;

/**
 * @author renarj
 */
@SpringBootApplication
@ComponentScan
@Import({EngineConfiguation.class, RestConfiguration.class, ServiceConfiguration.class})
public class JasDBNodeStarter {
    private static final Logger LOG = getLogger(JasDBNodeStarter.class);

    private static ConfigurableApplicationContext CONTEXT;

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        LOG.info("Starting JasDB Node");
        SpringApplicationBuilder builder = new SpringApplicationBuilder(JasDBNodeStarter.class)
                .bannerMode(Banner.Mode.OFF);
        CONTEXT = builder.run(args);
        ClusterManager clusterManager = CONTEXT.getBean(ClusterManager.class);
        if(clusterManager.join()) {
            LOG.error("Joined cluster");
        } else {
            LOG.error("Could not join cluster");
        }

        LOG.info("JasDB Node has started");
    }
}
