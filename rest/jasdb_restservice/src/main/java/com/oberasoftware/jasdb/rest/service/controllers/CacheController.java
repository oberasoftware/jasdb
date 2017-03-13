package com.oberasoftware.jasdb.rest.service.controllers;

import com.oberasoftware.jasdb.api.caching.CacheRegion;
import com.oberasoftware.jasdb.api.exceptions.RestException;
import com.oberasoftware.jasdb.core.caching.GlobalCachingMemoryManager;
import com.oberasoftware.jasdb.rest.model.CacheBucket;
import com.oberasoftware.jasdb.rest.model.CacheBucketCollection;
import com.oberasoftware.jasdb.rest.model.RestEntity;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.List;

@RestController
public class CacheController {
	@RequestMapping(value = "/Caches", method = RequestMethod.GET, produces = "application/json")
	public RestEntity loadModel() throws RestException {
        GlobalCachingMemoryManager cachingMemoryManager = GlobalCachingMemoryManager.getGlobalInstance();

        List<CacheBucket> buckets = new ArrayList<>();
        for(CacheRegion region : cachingMemoryManager.getRegions()) {
            buckets.add(new CacheBucket(region.name(), region.size(), region.memorySize()));
        }

        return new CacheBucketCollection(buckets);
	}
}
