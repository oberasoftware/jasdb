package com.oberasoftware.jasdb.core.statistics;

public class DummyStatRecord extends StatRecord {
	protected DummyStatRecord(StatTimeProvider timeProvider, String statName) {
		super(timeProvider, statName);
	}
	
	@Override
	public void start() {
		
	}

	@Override
	public void stop() {
		
	}

}
