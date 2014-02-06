package nl.renarj.jasdb.core.platform;

import nl.renarj.core.utilities.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ServiceLoader;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author Renze de Vries
 */
public class PlatformManagerFactory {
    private static final Logger LOG = LoggerFactory.getLogger(PlatformManagerFactory.class);

    private static final PlatformManagerFactory INSTANCE = new PlatformManagerFactory();

    private PlatformManager platformManager;

    private static final Lock LOCK = new ReentrantLock();

    private PlatformManagerFactory() {
        init();
    }

    private void init() {
        ServiceLoader<PlatformManager> platformManagerServiceLoader = ServiceLoader.load(PlatformManager.class);
        String platformManagerOverride = System.getProperty("jasdb.platform.manager");
        String systemJvm = System.getProperty("java.vm.name").toLowerCase();
        LOG.info("Using platform JVM: {}", systemJvm);
        for(PlatformManager loadedPlatformManager : platformManagerServiceLoader) {
            LOG.info("Platform manager: {}", loadedPlatformManager);
            if(loadedPlatformManager.platformMatch() && StringUtils.stringEmpty(platformManagerOverride)) {
                this.platformManager = loadedPlatformManager;
                break;
            } else if(StringUtils.stringNotEmpty(platformManagerOverride) && loadedPlatformManager.getClass().toString().equals(platformManagerOverride)) {
                this.platformManager = loadedPlatformManager;
                break;
            }
        }
        if(platformManager == null) {
            LOG.info("No platform specific platform manager found, using default");
            platformManager = new HotspotPlatformManager();
        }

        LOG.info("Using platform manager: {}", platformManager);
    }

    public static void setPlatformManager(PlatformManager platformManager) {
        LOCK.lock();
        try {
            INSTANCE.platformManager = platformManager;
        } finally {
            LOCK.unlock();
        }
    }

    public static PlatformManager getPlatformManager() {
        LOCK.lock();
        try {
            if(INSTANCE.platformManager == null) {
                INSTANCE.init();
            }
            return INSTANCE.platformManager;
        } finally {
            LOCK.unlock();
        }
    }
}
