package nl.renarj.jasdb.core.caching;

import nl.renarj.core.exceptions.CoreConfigException;
import nl.renarj.core.utilities.StringUtils;
import nl.renarj.core.utilities.configuration.Configuration;
import nl.renarj.core.utilities.conversion.ValueConverterUtil;
import nl.renarj.jasdb.core.exceptions.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Renze de Vries
 */
public class GlobalCachingMemoryManager {
    private static final Logger LOG = LoggerFactory.getLogger(GlobalCachingMemoryManager.class);

    private static final GlobalCachingMemoryManager INSTANCE = new GlobalCachingMemoryManager();

    private static final String DEFAULT_MONITOR_INTERVAL = "10s";

    private ConcurrentHashMap<String, CacheRegion<? extends Comparable, ? extends CacheEntry>> regionMap =
            new ConcurrentHashMap<>();

    private long maximumMemory;


    private CacheMonitorThread cacheMonitorThread;

    public GlobalCachingMemoryManager() {

    }

    public static GlobalCachingMemoryManager getGlobalInstance() {
        return INSTANCE;
    }

    public void configure(Configuration configuration) throws ConfigurationException {
        String maxMemoryValue = null;
        String monitorIntervalValue = DEFAULT_MONITOR_INTERVAL;
        if(configuration != null) {
             maxMemoryValue = configuration.getAttribute("MaxMemory");
             monitorIntervalValue = configuration.getAttribute("MemoryMonitorInterval", DEFAULT_MONITOR_INTERVAL);
        }

        long monitorInterval;
        try {
            monitorInterval = ValueConverterUtil.convertToMilliseconds(monitorIntervalValue);
            if(StringUtils.stringNotEmpty(maxMemoryValue)) {
                maximumMemory = ValueConverterUtil.convertToBytes(maxMemoryValue);
            } else {
                maximumMemory = Runtime.getRuntime().maxMemory();
            }
        } catch(CoreConfigException e) {
            throw new ConfigurationException("Unable to load caching settings", e);
        }

        LOG.info("Global cache monitor starting with interval: {} and memory limit: {} bytes", monitorInterval, maximumMemory);
        this.cacheMonitorThread = new CacheMonitorThread(this, monitorInterval, maximumMemory);
        this.cacheMonitorThread.start();
    }

    public static void shutdown() {
        GlobalCachingMemoryManager memoryManager = getGlobalInstance();
        memoryManager.regionMap.clear();
        if(memoryManager.cacheMonitorThread != null) {
            memoryManager.cacheMonitorThread.stop();
        }
    }

    public void checkMemoryState(Set<CacheRegion> ignoreRegions) {
        long memorySize = calculateMemorySize();
        if(memorySize > maximumMemory) {
            LOG.debug("Current memory size: {}", memorySize);
            LOG.debug("Maximum memory: {}", maximumMemory);
            long reduceSize = memorySize - maximumMemory;
            LOG.info("Memory overflow: {} bytes more than allowed limit", reduceSize);
            CacheRegion<? extends Comparable, ?> leastUsed = getLeastUsedRegion(ignoreRegions);
            if(leastUsed != null) {
                long actualReduce = leastUsed.reduceBy(reduceSize);
                LOG.debug("Reduced region: {} by: {} bytes", leastUsed, actualReduce);
                if(actualReduce < reduceSize) {
                    ignoreRegions.add(leastUsed);
                    LOG.debug("Reduce was not sufficient to meet reduce size: {} was actually: {}", reduceSize, actualReduce);
                    checkMemoryState(ignoreRegions);
                }
            } else {
                LOG.warn("Could not reduce memory footprint further, no more reducable regions available, current memory footprint: {}", calculateMemorySize());
            }
        }
    }

    private CacheRegion<? extends Comparable, ?> getLeastUsedRegion(Set<CacheRegion> checkedRegions) {
        CacheRegion<? extends Comparable, ?> leastUsed = null;
        for(CacheRegion<? extends Comparable, ?> region: getRegions()) {
            LOG.debug("Checking region: {}", region);
            if(!checkedRegions.contains(region)) {
                LOG.debug("Region available: {}", region.memorySize());
                if(leastUsed == null || leastUsed.lastRegionAccess() > region.lastRegionAccess()) {
                    leastUsed = region;
                }
            }
        }
        return leastUsed;
    }


    public <T extends Comparable<T>> void registerRegion(CacheRegion<T, ? extends CacheEntry> region) {
        regionMap.putIfAbsent(region.name(), region);
    }

    public long calculateMemorySize() {
        long total = 0;
        for(CacheRegion<? extends Comparable, ?> region: regionMap.values()) {
            total += region.memorySize();
        }
        return total;
    }


    public void unregisterRegion(String name) {
        regionMap.remove(name);
    }

    public List<CacheRegion> getRegions() {
        return new ArrayList<CacheRegion>(regionMap.values());
    }
}
