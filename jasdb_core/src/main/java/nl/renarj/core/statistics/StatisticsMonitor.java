package nl.renarj.core.statistics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;

public class StatisticsMonitor {
	private static StatisticsMonitor instance = new StatisticsMonitor();
	private static final Logger log = LoggerFactory.getLogger(StatisticsMonitor.class);
	
	private Queue<StatRecord> statRecords = new ConcurrentLinkedQueue<StatRecord>();
	private StatTimeProvider timeProvider;
	private Aggregator aggregator;
	
	private boolean enabled = false;
	
	private Map<String, AggregationResult> aggregations = new ConcurrentHashMap<String, AggregationResult>();
	
	private StatisticsMonitor() {
		this.timeProvider = new NanoTimeTimeProvider();
	}
	
	private void startAggregator() {
		if(!enabled) {
			this.aggregator = new AverageAggregator(statRecords);
			this.aggregator.start();
		}
	}
	
	private static StatisticsMonitor getInstance() {
		return instance;
	}
	
	public static void enableStatistics() {
		getInstance().startAggregator();
		getInstance().enabled = true;
	}
	
	public static void disableStatistics() {
		getInstance().aggregator.stop();
		getInstance().enabled = false;
		clearStats();
	}
	
	public static StatRecord createRecord(String action) {
		if(getInstance().enabled) {
			StatRecord record = new StatRecord(getInstance().timeProvider, action);
			record.start();
			
			getInstance().statRecords.add(record);
			
			return record;
		} else {
			return new DummyStatRecord(null, action);
		}
	}
	
	protected static StatTimeProvider getTimeProvider() {
		return getInstance().timeProvider;
	}
	
	protected static void addAggregationResult(AggregationResult aggregationResult) {
		getInstance().aggregations.put(aggregationResult.getName(), aggregationResult);
	}
	
	public static AggregationResult getAggregationResult(String action) {
		if(getInstance().aggregations.containsKey(action)) {
			return getInstance().aggregations.get(action);
		} else {
			return null;
		}
	}
	
	public static boolean hasAggregationResult(String action) {
		return getInstance().aggregations.containsKey(action);
	}
	
	public static List<AggregationResult> getAggregationResults() {
		return new ArrayList<AggregationResult>(getInstance().aggregations.values());
	}
	
	public static void clearStats() {
		getInstance().aggregations.clear();
		getInstance().statRecords.clear();
	}
	
	public static void logStats(TimeUnit timeUnit) {
		List<AggregationResult> aggregationResults = new ArrayList<AggregationResult>(getInstance().aggregations.values());
		Collections.sort(aggregationResults);
		
		for(AggregationResult result : aggregationResults) {
			//PrintStream average = System.out.printf("%10s", getTimeProvider().formatTime(result.getAverage(), timeUnit));
			long average = getTimeProvider().formatTime(result.getAverage(), timeUnit);
			long lowest = getTimeProvider().formatTime(result.getLowest(), timeUnit); 
			long highest = getTimeProvider().formatTime(result.getHighest(), timeUnit);
            long totalTime = getTimeProvider().formatTime(result.getTotalTime(), timeUnit);
			
			log.info("Aggregation result [average: {} lowest: {} highest: {} nr. actions: {} total time: {}] for: {}", new Object[] {
					 
					//getTimeProvider().formatTime(result.getAverage(), timeUnit),
					String.format("%6s", Long.toString(average)),
					String.format("%6s", Long.toString(lowest)), 
					String.format("%10s", Long.toString(highest)), 
					String.format("%6s", Long.toString(result.getCalls())),
                    String.format("%10s", Long.toString(totalTime)),
					result.getName()
			});
		}
	}
}
