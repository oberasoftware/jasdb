package com.oberasoftware.jasdb.service;

import com.oberasoftware.jasdb.api.engine.*;
import com.oberasoftware.jasdb.engine.metadata.JasDBMetadataStore;
import com.oberasoftware.jasdb.core.caching.GlobalCachingMemoryManager;
import com.oberasoftware.jasdb.api.exceptions.ConfigurationException;
import com.oberasoftware.jasdb.api.exceptions.JasDBException;
import com.oberasoftware.jasdb.api.model.NodeInformation;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.stereotype.Component;

import javax.management.*;
import java.io.IOException;
import java.io.InputStream;
import java.lang.management.ManagementFactory;
import java.net.URL;
import java.util.jar.Manifest;

import static org.slf4j.LoggerFactory.getLogger;

/**
 * @author renarj
 */
@Component
public class EngineManagerImpl implements EngineManager {
    private static final Logger LOG = getLogger(EngineManagerImpl.class);

    private static final String UNKNOWN_VERSION = "Unknown JasDB Version";
    private static final String RELEASE_VERSION = "ReleaseVersion";
    private static final String BUILD_NUMBER = "BuildNumber";

    private static final String UNABLE_TO_REGISTER_JMX_SHUTDOWN_HOOK = "Unable to register JMX shutdown hook";

    @Autowired
    private ConfigurationLoader configurationLoader;

    @Autowired
    private RemoteServiceManager remoteServiceManager;

    @Autowired
    private ConfigurableApplicationContext applicationContext;

    @Autowired
    private MetadataStore metadataStore;

    private String engineVersion;
    private NodeInformation nodeInformation;

    @Override
    public NodeInformation startEngine() throws JasDBException {
        LOG.info("Starting JasDB Database Engine");
        engineVersion = loadVersionData();

        GlobalCachingMemoryManager cachingMemoryManager = GlobalCachingMemoryManager.getGlobalInstance();
        Configuration cachingConfiguration = configurationLoader.getConfiguration().getChildConfiguration("/jasdb/caching");
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

        remoteServiceManager.startRemoteServices();

        this.nodeInformation = new NodeInformation(JasDBMetadataStore.PID, null);
        remoteServiceManager.getServiceInformation().forEach(r -> this.nodeInformation.addServiceInformation(r));
        return nodeInformation;
    }

    @Override
    public void stopEngine() {
        GlobalCachingMemoryManager.shutdown();

        MBeanServer server = ManagementFactory.getPlatformMBeanServer();
        try {
            ObjectName name = new ObjectName("nl.renarj.jasdb.core:type=KernelShutdown");
            server.unregisterMBean(name);
        } catch(MalformedObjectNameException | MBeanRegistrationException | InstanceNotFoundException e) {
            LOG.error("Unable to unregister management bean: {}", e.getMessage());
        }

        applicationContext.close();
    }

    @Override
    public String getEngineVersion() {
        return engineVersion;
    }

    @Override
    public NodeInformation getNodeInformation() {
        return this.nodeInformation;
    }

    @Override
    public MetadataStore getMetadataStore() {
        return metadataStore;
    }

    private String loadVersionData() {
        Class<EngineManagerImpl> kernelClass = EngineManagerImpl.class;
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
