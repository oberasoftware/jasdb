package nl.renarj.core.statistics;


public class AggregationResult implements Comparable<AggregationResult> {
	private String name;
    private long totalTime = 0;
	private long calls = 0;
	private long lowest = -1;
	private long highest = -1;
	private long start;
	
	public AggregationResult(String name, long start) {
		this.name = name;
		this.start = start;
	}
	
	protected void addResult(long interval) {
		if(interval > 0) {
            this.totalTime += interval;
			
			if(interval < lowest || lowest == -1) {
				this.lowest = interval;
			}
			
			if(highest < interval) {
				this.highest = interval;
			}
		} else {
			this.lowest = 0;
		}
		
		this.calls++;
	}

	public long getStart() {
		return start;
	}
	
	public String getName() {
		return name;
	}

	public long getAverage() {
		return totalTime / calls;
	}

    public long getTotalTime() {
        return totalTime;
    }

    public long getCalls() {
		return calls;
	}

	public long getLowest() {
		return lowest;
	}

	public long getHighest() {
		return highest;
	}
	
	@Override
	public int hashCode() {
		return getName().hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		if(obj instanceof AggregationResult) {
			return ((AggregationResult) obj).getName().equals(getName());
		} else {
			return false;
		}
	}

	@Override
	public int compareTo(AggregationResult o) {
		if(o != null) {
			return getName().compareTo(o.getName());
		} else {
			return -1;
		}
	}
}
