package nl.renarj.core.caching;

public class CacheConfig {
	private boolean enabled;
	private long maxMemSize;
	private int maxSize;
	
	protected CacheConfig(boolean enabled, long maxMemSize, int maxSize) {
		this.enabled = enabled;
		this.maxMemSize = maxMemSize;
		this.maxSize = maxSize;
	}

	public boolean isEnabled() {
		return enabled;
	}

	public long getMaxMemSize() {
		return maxMemSize;
	}

	public int getMaxSize() {
		return maxSize;
	}
}
