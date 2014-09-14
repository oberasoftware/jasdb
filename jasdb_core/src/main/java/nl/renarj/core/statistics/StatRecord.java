package nl.renarj.core.statistics;

public class StatRecord {
	private long start;
	private long end;
	private String name;
	
	private boolean finalized = false;
	
	private StatTimeProvider timeProvider;
	
	protected StatRecord(StatTimeProvider timeProvider, String statName) {
		this.name = statName;
		this.timeProvider = timeProvider;
	}
	
	public void start() {
		this.start = this.timeProvider.getCurrentTime();
	}
	
	public void stop() {
		this.end = this.timeProvider.getCurrentTime();
		this.finalized = true;
	}
	
	public boolean isFinalized() {
		return this.finalized;
	}
	
	public long interval() {
		return this.end - start;
	}
	
	public String getName() {
		return this.name;
	}
}
