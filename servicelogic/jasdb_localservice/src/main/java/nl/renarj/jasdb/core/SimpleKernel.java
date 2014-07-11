package nl.renarj.jasdb.core;

import nl.renarj.jasdb.api.model.DBInstanceFactory;
import nl.renarj.jasdb.core.caching.GlobalCachingMemoryManager;
import nl.renarj.jasdb.core.exceptions.ConfigurationException;
import nl.renarj.jasdb.core.exceptions.JasDBException;
import nl.renarj.jasdb.core.exceptions.JasDBStorageException;
import nl.renarj.jasdb.core.exceptions.LocatorException;
import nl.renarj.jasdb.core.exceptions.NoComponentFoundException;
import nl.renarj.jasdb.core.locator.NodeInformation;
import nl.renarj.jasdb.core.platform.PlatformManager;
import nl.renarj.jasdb.core.platform.PlatformManagerFactory;
import nl.renarj.jasdb.service.StorageServiceFactory;
import nl.renarj.jasdb.storage.exceptions.RecordStoreInUseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class SimpleKernel {
    private static final Logger LOG = LoggerFactory.getLogger(SimpleKernel.class);

    private static final Lock reconfigureLock = new ReentrantLock();
    private static SimpleKernel INSTANCE;

    private String instanceId = PlatformManagerFactory.getPlatformManager().getProcessId();
    private NodeInformation nodeInformation;
    private String kernelVersion = "unknown";

    private PlatformManager platformManager;

    private CountDownLatch latch = new CountDownLatch(1);

    private SimpleKernel() throws ConfigurationException {
		bootstrapKernel();
        LOG.debug("Loaded kernel with classloader instance: {}", getClass().getClassLoader().hashCode());
	}
	
	public static DBInstanceFactory getInstanceFactory() throws JasDBStorageException {
        return getInstance().platformManager.getComponent(DBInstanceFactory.class);
	}

	public static StorageServiceFactory getStorageServiceFactory() throws JasDBStorageException {
        return getInstance().platformManager.getComponent(StorageServiceFactory.class);
	}

    public static <T> T getKernelModule(Class<T> requiredModuleType) throws JasDBStorageException {
        return getInstance().platformManager.getComponent(requiredModuleType);
    }

    public static NodeInformation getNodeInformation() throws JasDBStorageException {
        try {
            return getInstance().nodeInformation;
        } catch(ConfigurationException e) {
            throw new JasDBStorageException("Unable to get node information: " + e.getMessage());
        }
    }
	
	private static SimpleKernel getInstance() throws ConfigurationException {
		if(INSTANCE == null) {
            loadKernel();
		}
		return INSTANCE;
	}
	
	public static void shutdown() throws JasDBException {
        reconfigureLock.lock();
        try {
            if(INSTANCE != null) {
                INSTANCE.kernelShutdown();
            }
        } finally {
            reconfigureLock.unlock();
        }
	}

    private void kernelShutdown() throws JasDBException {
        LOG.info("Shutting down kernel");


        List<RemoteService> remoteServices = platformManager.getComponents(RemoteService.class);
        if(remoteServices != null) {
            for(RemoteService remoteService : remoteServices) {
                LOG.debug("Stopping remote service endpoint: {}", remoteService.getClass().getName());
                remoteService.stopService();
            }
        }

        LOG.debug("Doing kernel shutdown, stopping instance and storage services");

        GlobalCachingMemoryManager.shutdown();

        LOG.info("KernelShutdown shutdown complete");
        INSTANCE.latch.countDown();
        INSTANCE = null;

        PlatformManagerFactory.getPlatformManager().shutdownPlatform();
    }

    /**
     * Public kernel bootstrap initializer, this guarantees the kernel gets started with all services
     * @throws ConfigurationException
     */
	public static void initializeKernel() throws ConfigurationException {
		getInstance();
	}

    public static void waitForShutdown() throws ConfigurationException {
        try {
            getInstance().latch.await();
        } catch(InterruptedException e) {
            LOG.info("Interrupted whilst waiting for kernel shutdown");
        }
    }

    /**
     * This performs the actual kernel loading in case it was not initialized
     * @throws ConfigurationException
     */
	private static void loadKernel() throws ConfigurationException {
        reconfigureLock.lock();
        try {
            if(INSTANCE == null) {
                INSTANCE = new SimpleKernel();
            }
        } finally {
            reconfigureLock.unlock();
        }
	}
    
	private void bootstrapKernel() throws ConfigurationException {
        LOG.info("Bootstrapping database kernel");


        platformManager = PlatformManagerFactory.getPlatformManager();
        LOG.info("Initializing platform: {}", platformManager);

        platformManager.initializePlatform();

        LOG.info("Finished platform initialization");

        try {
            this.nodeInformation = new NodeInformation(instanceId, null);

            try {
                List<RemoteService> remoteServices = platformManager.getComponents(RemoteService.class);
                if(remoteServices != null) {
                    for(RemoteService remoteService : remoteServices) {
                        if(remoteService.isEnabled()) {
                            this.nodeInformation.addServiceInformation(remoteService.getServiceInformation());

                            LOG.info("Starting remote service: {}", remoteService.getClass().getName());

                            remoteService.startService();
                        }
                    }
                }
            } catch(NoComponentFoundException e) {
                LOG.info("No remote service available");
            }

            kernelVersion = PlatformManagerFactory.getPlatformManager().getVersionData();
            LOG.info("Booting JasDB version: {}", kernelVersion);
            LOG.info("JasDB process id: {}", instanceId);
            registerShutdownHooks();

        } catch (LocatorException e) {
            handleBootupError("Unable to load kernel, unable to start locator service", e);
        } catch(RecordStoreInUseException e) {
            handleBootupError("Kernel cannot be loaded, DB is currently in use, are you running another JasDB instance on the same instance location?", e);
        } catch(JasDBException e) {
            handleBootupError("Unable to load kernel, storage services cannot be initialized", e);
        }
        LOG.info("Finished booting kernel, JASDB ready for usage...");
	}

    private void handleBootupError(String message, Throwable e) throws ConfigurationException {
        latch.countDown();
        String errorMessage = message + ": " + e.getMessage();
        LOG.error(message, e);
        throw new ConfigurationException(errorMessage);
    }

    private void registerShutdownHooks() throws JasDBStorageException {
        Thread shutdownThread = new Thread(new KernelShutdown());
        Runtime.getRuntime().addShutdownHook(shutdownThread);
    }

    public static String getVersion() throws ConfigurationException {
        return getInstance().kernelVersion;
    }
}
