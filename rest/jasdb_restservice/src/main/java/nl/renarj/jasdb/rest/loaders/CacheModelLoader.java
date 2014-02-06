package nl.renarj.jasdb.rest.loaders;

import nl.renarj.jasdb.api.context.RequestContext;
import nl.renarj.jasdb.core.caching.CacheRegion;
import nl.renarj.jasdb.core.caching.GlobalCachingMemoryManager;
import nl.renarj.jasdb.rest.exceptions.RestException;
import nl.renarj.jasdb.rest.exceptions.SyntaxException;
import nl.renarj.jasdb.rest.input.InputElement;
import nl.renarj.jasdb.rest.input.OrderParam;
import nl.renarj.jasdb.rest.model.CacheBucket;
import nl.renarj.jasdb.rest.model.CacheBucketCollection;
import nl.renarj.jasdb.rest.model.RestEntity;
import nl.renarj.jasdb.rest.serializers.RestResponseHandler;

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
