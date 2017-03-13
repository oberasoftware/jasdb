package com.oberasoftware.jasdb.rest.service.loaders;

import com.oberasoftware.jasdb.core.context.RequestContext;
import com.oberasoftware.jasdb.api.caching.CacheRegion;
import com.oberasoftware.jasdb.core.caching.GlobalCachingMemoryManager;
import com.oberasoftware.jasdb.api.exceptions.RestException;
import com.oberasoftware.jasdb.rest.service.exceptions.SyntaxException;
import com.oberasoftware.jasdb.rest.service.input.InputElement;
import com.oberasoftware.jasdb.rest.service.input.OrderParam;
import com.oberasoftware.jasdb.rest.model.CacheBucket;
import com.oberasoftware.jasdb.rest.model.CacheBucketCollection;
import com.oberasoftware.jasdb.rest.model.RestEntity;
import com.oberasoftware.jasdb.rest.model.serializers.RestResponseHandler;

import java.util.ArrayList;
import java.util.List;

public class CacheModelLoader  extends AbstractModelLoader {
	@Override
	public String[] getModelNames() {
		return new String[] {"Caches"};
	}

	@Override
	public RestEntity loadModel(InputElement input, String begin, String top, List<OrderParam> orderParamList, RequestContext context) throws RestException {
		if(input.getCondition() == null) {
                GlobalCachingMemoryManager cachingMemoryManager = GlobalCachingMemoryManager.getGlobalInstance();

				List<CacheBucket> buckets = new ArrayList<>();
				for(CacheRegion region : cachingMemoryManager.getRegions()) {
					buckets.add(new CacheBucket(region.name(), region.size(), region.memorySize()));
				}
				
				return new CacheBucketCollection(buckets);
		} else {
			throw new SyntaxException("Querying cache data is not supported");
		}
	}
	
	@Override
	public RestEntity writeEntry(InputElement input, RestResponseHandler serializer, String rawData, RequestContext context) throws RestException {
		throw new RestException("Write not supported on Cache data");
	}
}
