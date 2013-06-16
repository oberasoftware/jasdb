package nl.renarj.jasdb.core.platform;

import nl.renarj.jasdb.core.exceptions.RuntimeJasDBException;
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
        for(PlatformManager loadedPlatformManager : platformManagerServiceLoader) {
            if(loadedPlatformManager.platformMatch(systemJvm)) {
                this.platformManager = loadedPlatformManager;
                break;
            }
        }
        if(platformManager == null) {
            throw new RuntimeJasDBException("Could not load a suitable platform manager for jvm: " + systemJvm);
        } else {
            LOG.info("Using platform manager: {}", platformManager);
        }
    }

    public static PlatformManager getPlatformManager() {
        return platformManagerFactory.platformManager;
    }
}
