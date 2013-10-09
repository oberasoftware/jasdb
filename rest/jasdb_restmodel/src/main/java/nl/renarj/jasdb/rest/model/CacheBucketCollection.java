package nl.renarj.jasdb.rest.model;

import java.util.List;

public class CacheBucketCollection implements RestEntity {
	private List<CacheBucket> buckets;
	
	public CacheBucketCollection(List<CacheBucket> buckets) {
		this.buckets = buckets;
	}

	public List<CacheBucket> getBuckets() {
		return buckets;
	}

	public void setBuckets(List<CacheBucket> buckets) {
		this.buckets = buckets;
	}
}
