package nl.renarj.jasdb.core.platform;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
        LOG.info("Setting default platform manager to hotspot");
        platformManager = new HotspotPlatformManager();
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
