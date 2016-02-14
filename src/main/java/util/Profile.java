package util;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.StreamLogUtils;
import io.StreamUtils;
import net.sf.json.JSONObject;

public class Profile {
	private static Logger logger = LoggerFactory.getLogger(Profile.class);

	public static Profile instance = new Profile();

	private Map<String, String> args = new HashMap<String, String>();
	private ConcurrentHashMap<String, EventProfileBean> eventProfiles = new ConcurrentHashMap<String, EventProfileBean>();
	private ThreadLocal<Map<String, Long>> startTime = new ThreadLocal<Map<String, Long>>();
	ConcurrentHashMap<String, AtomicInteger> eCounters = new ConcurrentHashMap<String, AtomicInteger>();
	OutputStream os;

	volatile long invokeCount = 0;
	Timer timer = null;

	public void addArg(String arg, String val) {
		args.put(arg, val);
	}

	public void open(String file) throws FileNotFoundException {
		os = StreamUtils.outputStream(file);
	}

	public void startPeriodReport(String file, int period) throws FileNotFoundException {
		os = StreamUtils.outputStream(file);
		timer = new Timer();
		timer.schedule(new TimerTask() {
			@Override
			public void run() {
				flushAndReset();
			}
		}, period, period);
	}

	public void close() throws IOException {
		if (timer != null) {
			timer.cancel();
		}
		if (os != null) {
			os.close();
			os = null;
		}
	}

	public void flushAndReset() {
		try {
			StreamLogUtils.log(os, toJSON().toString());
		} catch (IOException e) {
			e.printStackTrace();
		}
		eventProfiles.clear();
		invokeCount = 0;
		eCounters.clear();

		lastActive = -1;
		Arrays.fill(visitCounts, 0);
		Arrays.fill(listCounts, 0);
	}

	public void reset() {
		eventProfiles.clear();
		if (startTime.get() != null)
			startTime.get().clear();
		invokeCount = 0;
		eCounters.clear();

		lastActive = -1;
		Arrays.fill(visitCounts, 0);
		Arrays.fill(listCounts, 0);
	}

	@Override
	public String toString() {
		return toJSON().toString();
	}

	public JSONObject toJSON() {
		JSONObject ret = new JSONObject();
		JSONObject obj = JSONObject.fromObject(eventProfiles);
		ret.put("perf", obj);
		obj = JSONObject.fromObject(eCounters);
		ret.put("ecounters", obj);
		ret.put("args", JSONObject.fromObject(args));
		return ret;
	}

	public void updateCounter(String counter) {
		updateCounter(counter, 1);
	}

	public void updateCounter(String counter, int count) {
		AtomicInteger ret = null;
		if (!eCounters.containsKey(counter)) {
			eCounters.putIfAbsent(counter, new AtomicInteger(0));
		}
		ret = eCounters.get(counter);
		if (ret != null)
			ret.addAndGet(count);
	}

	public static String toTimeTag(String tag) {
		return String.format("%s_time", tag);
	}

	public void start(String event) {
		if (startTime.get() == null) {
			startTime.set(new HashMap<String, Long>());
		}

		startTime.get().put(event, System.currentTimeMillis());
	}

	public void end(String event) {
		long gap = System.currentTimeMillis() - startTime.get().get(event);
		startTime.get().remove(event);

		if (!eventProfiles.containsKey(event)) {
			eventProfiles.putIfAbsent(event, new EventProfileBean());
		}
		EventProfileBean profile = eventProfiles.get(event);
		if (profile != null)
			profile.increment(gap);

		if (invokeCount++ % 50000 == 0) {
			// print();
		}
	}

	public void print() {
		System.out.println();
		for (Entry<String, EventProfileBean> profile : eventProfiles.entrySet()) {
			logger.info(profile.getKey() + " " + profile.getValue());
			// System.out.println(profile.getKey() + " " + profile.getValue());
		}
	}

	public int getCounter(String key) {
		if (!eCounters.containsKey(key))
			return 0;
		return eCounters.get(key).get();
	}

	HashMap<Integer, List<Integer>> visitGaps = new HashMap<Integer, List<Integer>>();

	public int newCursorID() {
		int ret = cursorIDGen++;
		curCursorID = ret;
		// visitGaps.put(ret, new ArrayList<Integer>());
		if (lastActive == -1)
			lastActive = ret;
		return ret;
	}

	public void active(int id) {
		curCursorID = id;
	}

	int cursorIDGen = 0;
	int lastActive = -1;
	int[] visitCounts = new int[50];
	int[] listCounts = new int[50];
	int curCursorID = -1;

	public void cursorAdvance() {
		// visitCounts[curCursorID - lastActive]++;
	}

	public void listAdvance() {
		// visitCounts[curCursorID - lastActive]++;
		// listCounts[curCursorID - lastActive]++;
	}

	public void nextBlock() {
		/*
		 * if (curCursorID >= 0) visitGaps.get(curCursorID).add(
		 * visitCounts[curCursorID - lastActive]);
		 */
	}

	public void printVisitPattern() {
		for (Entry<Integer, List<Integer>> entry : visitGaps.entrySet()) {
			System.out.println(entry);
			System.out.println("list counts:" + listCounts[entry.getKey() - lastActive]);
		}

		lastActive = -1;
		Arrays.fill(visitCounts, 0);
		Arrays.fill(listCounts, 0);
	}
}
