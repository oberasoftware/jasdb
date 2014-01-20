package nl.renarj.jasdb.core.platform;

import nl.renarj.core.utilities.configuration.Configuration;
import nl.renarj.jasdb.core.ConfigurationLoader;
import nl.renarj.jasdb.core.KernelShutdown;
import nl.renarj.jasdb.core.SimpleKernel;
import nl.renarj.jasdb.core.caching.GlobalCachingMemoryManager;
import nl.renarj.jasdb.core.exceptions.ConfigurationException;
import nl.renarj.jasdb.core.exceptions.NoComponentFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import javax.management.InstanceAlreadyExistsException;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;
import java.io.IOException;
import java.io.InputStream;
import java.lang.management.ManagementFactory;
import java.net.URL;
import java.util.jar.Manifest;

/**
 * @author Renze de Vries
 */
public class HotspotPlatformManager implements PlatformManager {
    private static final Logger LOG = LoggerFactory.getLogger(HotspotPlatformManager.class);

    private static final String UNABLE_TO_REGISTER_JMX_SHUTDOWN_HOOK = "Unable to register JMX shutdown hook";
    private static final String HOTSPOT_JVM = "hotspot";

    private static final String UNKNOWN_VERSION = "Unknown JasDB Version";
    private static final String RELEASE_VERSION = "ReleaseVersion";
    private static final String BUILD_NUMBER = "BuildNumber";

    private ApplicationContext applicationContext;

    @Override
    public boolean platformMatch() {
        return System.getProperty("java.vm.name").toLowerCase().contains(HOTSPOT_JVM);
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
    public void initializePlatform() throws ConfigurationException {
        applicationContext = new ClassPathXmlApplicationContext("META-INF/spring/app-context.xml");
        Configuration configuration = applicationContext.getBean(ConfigurationLoader.class).getConfiguration();

        GlobalCachingMemoryManager cachingMemoryManager = GlobalCachingMemoryManager.getGlobalInstance();
        Configuration cachingConfiguration = configuration.getChildConfiguration("/jasdb/caching");
        cachingMemoryManager.configure(cachingConfiguration);

        try {
            MBeanServer server = ManagementFactory.getPlatformMBeanServer();
            ObjectName name = new ObjectName("nl.renarj.jasdb.core:type=KernelShutdown");
            if(!server.isRegistered(name)) {
                server.registerMBean(new KernelShutdown(), name);
            }
        } catch (InstanceAlreadyExistsException | NotCompliantMBeanException | MalformedObjectNameException | MBeanRegistrationException e) {
            throw new ConfigurationException(UNABLE_TO_REGISTER_JMX_SHUTDOWN_HOOK, e);
        }

        ((ConfigurableApplicationContext)applicationContext).registerShutdownHook();
    }

    @Override
    public void shutdownPlatform() {
        MBeanServer server = ManagementFactory.getPlatformMBeanServer();
        try {
            ObjectName name = new ObjectName("nl.renarj.jasdb.core:type=KernelShutdown");
            server.unregisterMBean(name);
        } catch(MalformedObjectNameException | MBeanRegistrationException | InstanceNotFoundException e) {
            LOG.error("Unable to unregister management bean: {}", e.getMessage());
        }

        ((ConfigurableApplicationContext)applicationContext).close();
    }

    @Override
    public <T> T getComponent(Class<T> type) throws NoComponentFoundException {
        try {
            return applicationContext.getBean(type);
        } catch(NoSuchBeanDefinitionException e) {
            throw new NoComponentFoundException("Unable to find component", e);
        }
    }

    @Override
    public String getVersionData() {
        Class<SimpleKernel> kernelClass = SimpleKernel.class;
        String className = kernelClass.getSimpleName() + ".class";
        String classPath = kernelClass.getResource(className).toString();
        if(classPath.startsWith("jar")) {
            String manifestPath = classPath.substring(0, classPath.lastIndexOf("!") + 1) + "/META-INF/MANIFEST.MF";
            try {
                URL versionManifest = new URL(manifestPath);
                InputStream is = versionManifest.openStream();
                try {
                    Manifest mf = new Manifest(is);
                    String releaseVersion = mf.getMainAttributes().getValue(RELEASE_VERSION);
                    String builderNumber = mf.getMainAttributes().getValue(BUILD_NUMBER);

                    return releaseVersion + "-" + builderNumber;
                } finally {
                    if(is != null) {
                        is.close();
                    }
                }
            } catch(IOException e) {
                LOG.warn("Unable to load kernel version information, ignoring", e);
            }
        } else {
            LOG.info("No kernel versioning information available, not loading from jar");
        }

        return UNKNOWN_VERSION;
    }
}
