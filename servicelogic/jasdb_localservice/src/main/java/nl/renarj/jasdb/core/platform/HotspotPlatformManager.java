package nl.renarj.jasdb.core.platform;

import nl.renarj.jasdb.core.KernelShutdown;
import nl.renarj.jasdb.core.exceptions.JasDBStorageException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.InstanceAlreadyExistsException;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;

/**
 * @author Renze de Vries
 */
public class HotspotPlatformManager implements PlatformManager {
    private static final Logger LOG = LoggerFactory.getLogger(HotspotPlatformManager.class);

    private static final String UNABLE_TO_REGISTER_JMX_SHUTDOWN_HOOK = "Unable to register JMX shutdown hook";
    private static final String HOTSPOT_JVM = "hotspot";

    @Override
    public boolean platformMatch(String platformName) {
        return platformName.contains(HOTSPOT_JVM);
    }

    @Override
    public String getDefaultStorageLocation() {
        return System.getProperty("user.home");
    }

    @Override
    public String getProcessId() {
        return ManagementFactory.getRuntimeMXBean().getName();
    }

    @Override
    public void initializePlatform() throws JasDBStorageException {
        try {
            MBeanServer server = ManagementFactory.getPlatformMBeanServer();
            ObjectName name = new ObjectName("nl.renarj.jasdb.core:type=KernelShutdown");
            if(!server.isRegistered(name)) {
                server.registerMBean(new KernelShutdown(), name);
            }
        } catch (InstanceAlreadyExistsException e) {
            throw new JasDBStorageException(UNABLE_TO_REGISTER_JMX_SHUTDOWN_HOOK, e);
        } catch (MBeanRegistrationException e) {
            throw new JasDBStorageException(UNABLE_TO_REGISTER_JMX_SHUTDOWN_HOOK, e);
        } catch (NotCompliantMBeanException e) {
            throw new JasDBStorageException(UNABLE_TO_REGISTER_JMX_SHUTDOWN_HOOK, e);
        } catch(MalformedObjectNameException e) {
            throw new JasDBStorageException(UNABLE_TO_REGISTER_JMX_SHUTDOWN_HOOK, e);
        }
    }

    @Override
    public void shutdownPlatform() {
        MBeanServer server = ManagementFactory.getPlatformMBeanServer();
        try {
            ObjectName name = new ObjectName("nl.renarj.jasdb.core:type=KernelShutdown");
            server.unregisterMBean(name);
        } catch(MalformedObjectNameException e) {
            LOG.error("Unable to unregister management bean: {}", e.getMessage());
        } catch(MBeanRegistrationException e) {
            LOG.error("Unable to unregister management bean: {}", e.getMessage());
        } catch(InstanceNotFoundException e) {
            LOG.error("Unable to unregister management bean: {}", e.getMessage());
        }
    }
}
