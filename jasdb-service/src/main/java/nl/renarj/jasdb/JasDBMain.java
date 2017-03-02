/*
 * The JASDB software and code is Copyright protected 2012 and owned by Renze de Vries
 * 
 * All the code and design principals in the codebase are also Copyright 2012 
 * protected and owned Renze de Vries. Any unauthorized usage of the code or the 
 * design and principals as in this code is prohibited.
 */
package nl.renarj.jasdb;

import nl.renarj.jasdb.core.KernelShutdownMBean;
import nl.renarj.jasdb.core.SimpleKernel;
import nl.renarj.jasdb.core.exceptions.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.JMX;
import javax.management.MBeanServerConnection;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import java.io.IOException;
import java.net.MalformedURLException;

/**
 * User: renarj
 * Date: 4/30/12
 * Time: 8:57 PM
 */
public class JasDBMain {
    private static final Logger log = LoggerFactory.getLogger(JasDBMain.class);

    private static void sendShutdownSignal() {
        try {
            log.info("Sending shutdown signal");
            JMXServiceURL url = new JMXServiceURL("service:jmx:rmi:///jndi/rmi://localhost:9999/jmxrmi");
            JMXConnector jmxc = JMXConnectorFactory.connect(url, null);
            MBeanServerConnection mbsc = jmxc.getMBeanServerConnection();

            ObjectName mbeanName = new ObjectName("nl.renarj.jasdb.core:type=KernelShutdown");
            KernelShutdownMBean kernelShutdownBean = JMX.newMBeanProxy(mbsc, mbeanName, KernelShutdownMBean.class, true);
            if(kernelShutdownBean != null) {
                log.info("JasDB KernelShutdown hook is aquired, sending shutdown signal");
                kernelShutdownBean.doKernelShutdown();
            } else {
                log.info("JasDB kernel is not running");
            }
        } catch(MalformedURLException e) {
            log.error("Unable to send shutdown signal: " + e.getMessage());
        } catch (IOException e) {
            log.error("Unable to send shutdown signal: " + e.getMessage());
        } catch (MalformedObjectNameException e) {
            log.error("Unable to send shutdown signal: " + e.getMessage());
        }
    }

    public static void main(String[] args) {
        if(args.length > 0) {
            if("--stop".equals(args[0])) {
                sendShutdownSignal();
            } else {
                log.info("Only supported command --stop");
            }
        } else {
            log.info("Triggering kernel initialization");
            try {
                SimpleKernel.initializeKernel();
                SimpleKernel.waitForShutdown();
            } catch(ConfigurationException e) {
                log.error("Unable to initialize DB kernel", e);
            }
        }
    }
}
