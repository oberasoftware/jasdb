package com.oberasoftware.jasdb.service;

import com.oberasoftware.jasdb.api.engine.EngineManager;
import com.oberasoftware.jasdb.api.exceptions.JasDBException;
import com.oberasoftware.jasdb.api.exceptions.JasDBStorageException;
import com.oberasoftware.jasdb.api.model.NodeInformation;
import com.oberasoftware.jasdb.engine.EngineConfiguation;
import com.oberasoftware.jasdb.rest.service.RestConfiguration;
import com.oberasoftware.jasdb.rest.service.RestConfigurationLoader;
import org.slf4j.Logger;
import org.springframework.boot.Banner;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.web.EmbeddedServletContainerAutoConfiguration;
import org.springframework.boot.autoconfigure.web.WebMvcAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Import;

import javax.management.JMX;
import javax.management.MBeanServerConnection;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import static org.slf4j.LoggerFactory.getLogger;

/**
 * @author renarj
 */
@SpringBootApplication(exclude = {EmbeddedServletContainerAutoConfiguration.class, WebMvcAutoConfiguration.class})
@Import({RestConfiguration.class, EngineConfiguation.class, ServiceConfiguration.class})
public class JasDBMain {
    private static final Logger LOG = getLogger(JasDBMain.class);

    private static CountDownLatch LATCH;
    private static ConfigurableApplicationContext CONTEXT;

    public static void main(String[] args) throws JasDBException {
        if(args.length > 0) {
            if("--stop".equals(args[0])) {
                shutdown();
            } else {
                LOG.info("Only supported command --stop");
            }
        } else {
            start(args);
        }
    }

    public static void start() throws JasDBException {
        start(new String[]{});
    }

    public static synchronized void start(String[] args) throws JasDBException {
        if(CONTEXT == null) {
            LOG.info("Starting JaSDB");
            SpringApplicationBuilder builder = new SpringApplicationBuilder(JasDBMain.class)
                    .bannerMode(Banner.Mode.OFF).web(RestConfigurationLoader.isEnabled());
            CONTEXT = builder.run(args);
            LATCH = new CountDownLatch(1);
            registerShutdownHooks();

            EngineManager engineManager = CONTEXT.getBean(EngineManager.class);
            NodeInformation nodeInformation = engineManager.startEngine();
            LOG.info("JasDB started: {}", nodeInformation);
        } else {
            LOG.info("JasDB was already started");
        }
    }

    public static boolean isStarted() {
        return CONTEXT != null;
    }

    public static void waitForShutdown() {
        try {
            LATCH.await();
        } catch(InterruptedException e) {
            LOG.info("Interrupted whilst waiting for kernel shutdown");
        }
    }

    private static void registerShutdownHooks() throws JasDBStorageException {
        Thread shutdownThread = new Thread(new KernelShutdown());
        Runtime.getRuntime().addShutdownHook(shutdownThread);
    }

    public static void shutdown() {
        if(CONTEXT != null) {
            LOG.info("Shutting down in process JasDB instance");
            CONTEXT.close();
            CONTEXT = null;

            LATCH.countDown();
        } else {
            try {
                LOG.info("Sending shutdown signal");
                JMXServiceURL url = new JMXServiceURL("service:jmx:rmi:///jndi/rmi://localhost:9999/jmxrmi");
                JMXConnector jmxc = JMXConnectorFactory.connect(url, null);
                MBeanServerConnection mbsc = jmxc.getMBeanServerConnection();

                ObjectName mbeanName = new ObjectName("nl.renarj.jasdb.core:type=KernelShutdown");
                KernelShutdownMBean kernelShutdownBean = JMX.newMBeanProxy(mbsc, mbeanName, KernelShutdownMBean.class, true);
                if (kernelShutdownBean != null) {
                    LOG.info("JasDB KernelShutdown hook is aquired, sending shutdown signal");
                    kernelShutdownBean.doKernelShutdown();
                } else {
                    LOG.info("JasDB kernel is not running");
                }
            } catch (IOException | MalformedObjectNameException e) {
                LOG.error("Unable to send shutdown signal, could not connect to JasDB instance: " + e.getMessage());
            }
        }
    }

}
