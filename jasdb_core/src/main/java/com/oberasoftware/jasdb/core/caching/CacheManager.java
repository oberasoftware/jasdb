package com.oberasoftware.jasdb.core.caching;

import com.oberasoftware.jasdb.api.caching.Bucket;
import com.oberasoftware.jasdb.api.exceptions.CoreConfigException;
import com.oberasoftware.jasdb.api.model.CacheConfig;
import com.oberasoftware.jasdb.api.engine.Configuration;
import com.oberasoftware.jasdb.core.utils.conversion.ValueConverterUtil;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public final class CacheManager {
	/* configuration attributes */
	private static final String CACHE_ENABLED = "Enabled";

	/* default values */
	private static final String DEFAULT_SIZE = "1024";
	private static final String DEFAULT_MEM_SIZE = "128m";
	private static final boolean DEFAULT_ENABLED = false;
	
	private CacheConfig defaultConfig;
	
	private final Map<String, Bucket> buckets;
	
	private final Map<String, CacheConfig> bucketConfigs;
	
	public CacheManager() {
		this.buckets = new ConcurrentHashMap<>();
		this.bucketConfigs = new HashMap<>();
		this.defaultConfig = new CacheConfig(false, 0, 0);
	}
	
	public void configure(Configuration config) throws CoreConfigException {
		this.defaultConfig = getCacheConfig(config);
		List<Configuration> bucketConfigs = config.getChildConfigurations("Bucket");
		for(Configuration bucketConfig : bucketConfigs) {
			String bucketName = bucketConfig.getAttribute("Name");
			CacheConfig bucketCacheConfig = getCacheConfig(bucketConfig);
			this.bucketConfigs.put(bucketName, bucketCacheConfig);
		}
	}
	
	private CacheConfig getCacheConfig(Configuration config) throws CoreConfigException {
		boolean enabled = config.getAttribute(CACHE_ENABLED, DEFAULT_ENABLED);
		String memSize = getValue(config, "MaxCacheMemSize", DEFAULT_MEM_SIZE);
		String size = getValue(config, "MaxItems", DEFAULT_SIZE);

		CacheConfig cacheConfig = new CacheConfig(enabled, 
				ValueConverterUtil.convertToBytes(memSize, 134217728L),
				ValueConverterUtil.safeConvertInteger(size, 1024));
		
		return cacheConfig;
	}
	
	private String getValue(Configuration config, String property, String defaultValue) {
		Configuration propertyConfig = config.getChildConfiguration("./Property[@Name='" + property + "']");
		if(propertyConfig != null) {
			String propertyValue = propertyConfig.getAttribute("Value");
		
			if(propertyValue != null) {
				return propertyValue;
			}
		}
		
		return defaultValue;
	}
	
	public Bucket getBucket(String bucketName) throws CoreConfigException {
		return createOrGetBucket(bucketName);
	}
	
	public List<Bucket> getBuckets() {
		return new ArrayList<Bucket>(buckets.values());
	}
	
	public void shutdownCacheManager() {
		for(Bucket bucket : buckets.values()) {
			bucket.closeBucket();
		}
		
		buckets.clear();
	}
	
	public void startCacheManager() {
	}
	
	private Bucket createOrGetBucket(String bucketName) throws CoreConfigException {
		if(!buckets.containsKey(bucketName)) {
			CacheConfig usedConfig = null;
			if(bucketConfigs.containsKey(bucketName)) {
				usedConfig = bucketConfigs.get(bucketName);
			} else {
				usedConfig = defaultConfig;
			}
			
			Bucket bucket;
			if(usedConfig.isEnabled()) {
				 bucket = new CacheBucket(bucketName);
			} else {
				bucket = new DummyBucket();
			}
			bucket.configure(usedConfig);
			buckets.put(bucketName, bucket);
			return bucket;
		} else {
			return buckets.get(bucketName);
		}
	}
}
