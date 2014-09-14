package nl.renarj.core.statistics;

public interface Aggregator extends Runnable {
	public void start();
	public void stop();
}