package nl.renarj.jasdb.core.platform;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ServiceLoader;

/**
 * @author Renze de Vries
 */
public class PlatformManagerFactory {
    private static final Logger LOG = LoggerFactory.getLogger(PlatformManagerFactory.class);

    private static final PlatformManagerFactory platformManagerFactory = new PlatformManagerFactory();

    private PlatformManager platformManager;

    private PlatformManagerFactory() {
        ServiceLoader<PlatformManager> platformManagerServiceLoader = ServiceLoader.load(PlatformManager.class);
        String systemJvm = System.getProperty("java.vm.name").toLowerCase();
        LOG.info("Using platform JVM: {}", systemJvm);
        for(PlatformManager loadedPlatformManager : platformManagerServiceLoader) {
            LOG.info("Platform manager: {}", loadedPlatformManager);
            if(loadedPlatformManager.platformMatch(systemJvm)) {
                this.platformManager = loadedPlatformManager;
                break;
            }
        }
        if(platformManager == null) {
            platformManager = new DefaultPlatformManager();
        }

        LOG.info("Using platform manager: {}", platformManager);
    }

    public synchronized static void setPlatformManager(PlatformManager platformManager) {
        platformManagerFactory.platformManager = platformManager;
    }

    public static PlatformManager getPlatformManager() {
        return platformManagerFactory.platformManager;
    }
}
