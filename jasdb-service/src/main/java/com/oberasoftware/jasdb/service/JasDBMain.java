package com.oberasoftware.jasdb.service;

import com.oberasoftware.jasdb.engine.EngineConfiguation;
import nl.renarj.jasdb.api.engine.EngineManager;
import nl.renarj.jasdb.core.exceptions.JasDBException;
import nl.renarj.jasdb.core.locator.NodeInformation;
import nl.renarj.jasdb.rest.RestConfiguration;
import org.slf4j.Logger;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Import;

import javax.management.JMX;
import javax.management.MBeanServerConnection;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import java.io.IOException;

import static org.slf4j.LoggerFactory.getLogger;

/**
 * @author renarj
 */
@SpringBootApplication
@ComponentScan
@Import({RestConfiguration.class, EngineConfiguation.class})
public class JasDBMain {
    private static final Logger LOG = getLogger(JasDBMain.class);



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

    public static void start(String[] args) throws JasDBException {
        LOG.info("Starting JaSDB");

        ApplicationContext context = SpringApplication.run(JasDBMain.class, args);
        EngineManager engineManager = context.getBean(EngineManager.class);
        NodeInformation nodeInformation = engineManager.startEngine();
        LOG.info("JasDB started: {}", nodeInformation);
    }

    public static void shutdown() {
        try {
            LOG.info("Sending shutdown signal");
            JMXServiceURL url = new JMXServiceURL("service:jmx:rmi:///jndi/rmi://localhost:9999/jmxrmi");
            JMXConnector jmxc = JMXConnectorFactory.connect(url, null);
            MBeanServerConnection mbsc = jmxc.getMBeanServerConnection();

            ObjectName mbeanName = new ObjectName("nl.renarj.jasdb.core:type=KernelShutdown");
            KernelShutdownMBean kernelShutdownBean = JMX.newMBeanProxy(mbsc, mbeanName, KernelShutdownMBean.class, true);
            if(kernelShutdownBean != null) {
                LOG.info("JasDB KernelShutdown hook is aquired, sending shutdown signal");
                kernelShutdownBean.doKernelShutdown();
            } else {
                LOG.info("JasDB kernel is not running");
            }
        } catch (IOException | MalformedObjectNameException e) {
            LOG.error("Unable to send shutdown signal: " + e.getMessage());
        }
    }

}
