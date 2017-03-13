package com.oberasoftware.jasdb.core.statistics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Queue;

public class AverageAggregator implements Aggregator {
	private static final Logger LOG = LoggerFactory.getLogger(AverageAggregator.class);
	
	private static final int SLEEP_INTERVAL = 100;
	
	private boolean running = false;
	private Thread aggregatorThread;
	
	private Queue<StatRecord> recordQueue;
	
	protected AverageAggregator(Queue<StatRecord> recordQueue) {
		this.recordQueue = recordQueue;
	}
	
	public void start() {
		this.running = true;
		this.aggregatorThread = new Thread(this);
        this.aggregatorThread.setDaemon(true);
		this.aggregatorThread.start();
	}
	
	public void stop() {
		this.running = false;
		aggregatorThread.interrupt();
		try {
			LOG.debug("Waiting for Statistics aggregator thread to shutdown");
			aggregatorThread.join();
			LOG.info("Statistics aggregator thread has shutdown cleanly");
		} catch(InterruptedException e) {
			LOG.error("Interrupted while waiting for thread join", e);
		}
	}
	
	public void run() {
		while(running) {
            try {
                StatRecord record = this.recordQueue.peek();
                if(record != null && record.isFinalized()) {
                    record = this.recordQueue.poll();
                    if(record != null) {
                        processRecord(record);
                    }
                } else if(record == null) {
                    try {
                        Thread.sleep(SLEEP_INTERVAL);
                    } catch(InterruptedException e) {
                        LOG.debug("Sleep was interrupted on stat monitor");
                    }
                } else if(!record.isFinalized()) {
                    this.recordQueue.remove();
                    this.recordQueue.add(record);
                }
            } catch(Throwable e) {
                LOG.error("Fatal error in statistics collection", e);
            }
		}
	}
	
	private void processRecord(StatRecord record) {
		String recordName = record.getName();
		if(!StatisticsMonitor.hasAggregationResult(recordName)) {
			StatisticsMonitor.addAggregationResult(new AggregationResult(recordName, System.currentTimeMillis()));
		}
		AggregationResult aggregation = StatisticsMonitor.getAggregationResult(recordName);
		aggregation.addResult(record.interval());
	}
}
