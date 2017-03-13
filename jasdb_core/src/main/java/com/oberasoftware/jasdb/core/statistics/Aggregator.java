package com.oberasoftware.jasdb.core.statistics;

public interface Aggregator extends Runnable {
	void start();
	void stop();
}
