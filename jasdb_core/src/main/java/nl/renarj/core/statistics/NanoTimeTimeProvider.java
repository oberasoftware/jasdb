package nl.renarj.core.statistics;

import java.util.concurrent.TimeUnit;

public class NanoTimeTimeProvider implements StatTimeProvider {

	@Override
	public long getCurrentTime() {
		return System.nanoTime();
	}

	@Override
	public long formatTime(long timeDifference, TimeUnit unit) {
		return unit.convert(timeDifference, TimeUnit.NANOSECONDS);
	}

}
