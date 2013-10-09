package nl.renarj.jasdb.index.btreeplus.caching;

import nl.renarj.core.exceptions.CoreConfigException;
import nl.renarj.core.utilities.configuration.Configuration;
import nl.renarj.core.utilities.conversion.ValueConverterUtil;
import nl.renarj.jasdb.core.exceptions.ConfigurationException;

/**
 * @author Renze de Vries
 */
public class CachingConfig {
    private static final long DEFAULT_MAX_MEMSIZE = -1;
    private static final long DEFAULT_MAX_BLOCKS = -1;

    private long maxMemSize;
    private long maxBlocks;

    public CachingConfig(long maxMemSize, long maxBlocks) {
        this.maxBlocks = maxBlocks;
        this.maxMemSize = maxMemSize;
    }

    public long getMaxMemSize() {
        return maxMemSize;
    }

    public long getMaxBlocks() {
        return maxBlocks;
    }

    public static CachingConfig getDefaultCachingConfig() {
        return new CachingConfig(DEFAULT_MAX_MEMSIZE, DEFAULT_MAX_BLOCKS);
    }

    public static CachingConfig createCachingConfig(Configuration config) throws ConfigurationException {
        long maxMemSize = DEFAULT_MAX_MEMSIZE;
        long maxBlocks = DEFAULT_MAX_BLOCKS;
        if(config != null) {
            Configuration maxCacheMemSizeConfig = config.getChildConfiguration("Property[@Name='MaxCacheMemSize']");
            if(maxCacheMemSizeConfig != null) {
                try {
                    maxMemSize = ValueConverterUtil.convertToBytes(maxCacheMemSizeConfig.getAttribute("Value"), DEFAULT_MAX_MEMSIZE);
                } catch(CoreConfigException e) {
                    maxMemSize = DEFAULT_MAX_MEMSIZE;
                }
            }

            Configuration maxCachedBlocksConfig = config.getChildConfiguration("Property[@Name='MaxCachedBlocks']");
            if(maxCachedBlocksConfig != null) {
                try {
                    maxBlocks = Long.valueOf(maxCachedBlocksConfig.getAttribute("Value", String.valueOf(DEFAULT_MAX_BLOCKS)));
                } catch(NumberFormatException e) {
                    maxBlocks = DEFAULT_MAX_BLOCKS;
                }
            }
        }

        return new CachingConfig(maxMemSize, maxBlocks);
    }
}
