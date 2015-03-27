package Util;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class EventProfileBean {
	private AtomicInteger count = new AtomicInteger(0);
	private AtomicLong totalTime = new AtomicLong(0);

	public EventProfileBean() {
	}

	public void increment(long time) {
		count.incrementAndGet();
		totalTime.addAndGet(time);
	}

	public int getCount() {
		return count.get();
	}

	public void setCount(int count) {
		this.count.set(count);
	}

	public long getTotalTime() {
		return totalTime.get();
	}

	public void setTotalTime(long totalTime) {
		this.totalTime.set(totalTime);
	}

	@Override
	public String toString() {
		return String.format("count:%d;totalTime:%d", count.get(),
				totalTime.get());
	}
}