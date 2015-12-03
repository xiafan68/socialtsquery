package expr;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import org.apache.commons.collections.Factory;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import core.lsmt.LSMTInvertedIndex;
import net.sf.json.JSONObject;
import segmentation.Interval;
import util.Configuration;
import util.Profile;
import xiafan.util.Pair;
import xiafan.util.StreamLogUtils;
import xiafan.util.StreamUtils;
import xiafan.util.collection.DefaultedPutMap;

public class DiskBasedPerfTest {
	private static final Logger logger = Logger.getLogger(DiskBasedPerfTest.class);
	QueryGen gen = new QueryGen(10);
	LSMTInvertedIndex index;

	public DiskBasedPerfTest() {
	}

	public void loadQuery() throws IOException {
		// /home/xiafan/KuaiPan/dataset/time_series/nqueryseed.txt
		gen.loadQueryWithTime("/home/xiafan/dataset/twitter/tqueryseed.txt");
	}

	public static LSMTInvertedIndex load(String confFile) throws IOException {
		PropertyConfigurator.configure("./conf/log4j-server.properties");
		Configuration conf = new Configuration();
		conf.load(confFile);
		LSMTInvertedIndex index = new LSMTInvertedIndex(conf);
		index.init();
		return index;
	}

	/*
	 * private final int[] widths = new int[] { 0, 24, 24 * 28 }; int[] ks = new
	 * int[] { 10, 50, 100, 300, 400 }; private final int[] offsets = new int[]
	 * { 0, 24 * 7, 24 * 30 };
	 */
	// complete test

	public static final int[] widths = new int[] { 2, 8, 12, 24, 48, 48 * 7, 48 * 30 };
	public static final int[] ks = new int[] { 10, 20, 50, 100, 150, 200, 250, 300, 350, 400 };
	public static final int[] offsets = new int[] { 0, 2, 12, 24, 48, 48 * 7, 48 * 30 };

	/**
	 * 在inverted index上面执行一边所有选取的查询
	 * 
	 * @param offset
	 * @param width
	 * @param k
	 * @return
	 * @throws IOException
	 */
	private HashMap<String, Double> testOneRound(int offset, int width, int k) throws IOException {
		HashMap<String, Double> counter = new HashMap<String, Double>();
		DefaultedPutMap<String, Double> map = DefaultedPutMap.decorate(counter, new Factory() {
			@Override
			public Object create() {
				return new Double(0);
			}
		});
		gen.resetCur();
		while (gen.hasNext()) {
			Pair<List<String>, Integer> query = gen.nextQuery();
			List<String> keywords = query.arg0;
			int start = query.arg1 + offset;
			try {
				index.query(keywords, start, start + width, k, "WEIGHTED");
			} catch (Exception ex) {
				logger.error(k + " " + start + " " + (start + width) + " " + keywords.size());
			}
			JSONObject perf = Profile.instance.toJSON();
			logger.info(offset + " " + width + " " + k + " " + perf);
			updateProfile(map, perf);
			Profile.instance.reset();
			Runtime.getRuntime().exec("sync && echo 1 > /proc/sys/vm/drop_caches");
		}
		System.gc();
		return counter;
	}

	public void test(String conf, File oFile) throws ParseException, IOException {
		// File dirFile = new File(oDir);
		// if (!dirFile.exists())
		// dirFile.mkdirs();
		index = load(conf);
		testOneRound(0, 100, 100);
		File logFile = oFile;
		if (logFile.exists())
			logFile.delete();
		OutputStream os = StreamUtils.outputStream(logFile);
		for (int offset : offsets) {
			List<String> logs = new ArrayList<String>();
			for (int width : widths) {
				for (int k : ks) {
					HashMap<String, Double> counter = testOneRound(offset, width, k);
					HashMap<String, Object> profile = new HashMap<String, Object>(normalize(counter, gen.size()));
					profile.put("width", width);
					profile.put("words", 2);
					profile.put("k", k);
					profile.put("offset", offset);
					logger.info(JSONObject.fromObject(profile));
					logs.add(JSONObject.fromObject(profile).toString());
					try {
						Thread.sleep(100);
					} catch (InterruptedException e) {
					}
				}
			} // end of widths
			for (String log : logs)
				StreamLogUtils.log(os, log);
			os.flush();
		}
		os.close();
		index.close();
	}

	private static double getValue(JSONObject obj) {
		return obj.getDouble("totalTime") / obj.getDouble("count");
	}

	static final String[] pFields = new String[] { Profile.TOTAL_TIME };

	static final String[] candFields = new String[] { Profile.CAND, };

	static final String[] IOFields = new String[] { Profile.NUM_BLOCK };

	static final String[] IOTimeFields = new String[] { Profile.READ_BLOCK };

	/**
	 * 将新测试的代价更新到map中去
	 * 
	 * @param map
	 * @param perf
	 */
	public static void updateProfile(DefaultedPutMap<String, Double> map, JSONObject perf) {
		JSONObject cur = perf.getJSONObject("perf");
		JSONObject obj = null;

		for (String field : pFields) {
			obj = cur.getJSONObject(field);
			double time = 0.0;
			if (obj.containsKey("totalTime"))
				time = obj.getDouble("totalTime");
			map.put(field, map.get(field) + time);
		}

		for (String field : IOTimeFields) {
			obj = cur.getJSONObject(field);
			double time = 0.0;
			if (obj.containsKey("totalTime"))
				time = obj.getDouble("totalTime");
			map.put(field, map.get(field) + time);
		}

		// counters
		cur = perf.getJSONObject("ecounters");
		for (String field : candFields) {
			double time = 0.0;
			if (cur.containsKey(field))
				time = cur.getDouble(field);
			map.put(field, map.get(field) + time);
		}

		for (String field : IOFields) {
			double time = 0.0;
			if (cur.containsKey(field))
				time = cur.getDouble(field);
			map.put(field, map.get(field) + time);
		}
	}

	/**
	 * 求多次测试的平均值
	 * 
	 * @param counter
	 * @param num
	 * @return
	 */
	public static HashMap<String, Double> normalize(HashMap<String, Double> counter, int num) {
		HashMap<String, Double> ret = new HashMap<String, Double>();
		for (Entry<String, Double> entry : counter.entrySet()) {
			ret.put(entry.getKey(), entry.getValue() / (float) num);
		}
		float ioTime = 0;
		for (String time : IOTimeFields) {
			ioTime += ret.get(time);
		}
		return ret;
	}

	public void test(int start, int width, int k, int iter, String conf, List<String> keywords)
			throws ParseException, IOException {
		LSMTInvertedIndex index = load(conf);
		for (int i = 0; i < iter; i++) {
			Iterator<Interval> invs = index.query(keywords, start, start + width, k, "WEIGHTED");
			JSONObject obj = Profile.instance.toJSON();
			obj.put("words", keywords.size());
			obj.put("width", width);
			obj.put("k", k);
			if (iter == 1 || i >= 1)
				System.out.println(obj);
			Profile.instance.reset();
		}
		index.close();
		System.gc();
		try {
			Thread.sleep(10);
		} catch (InterruptedException e) {
		}
	}

	public static void main(String[] args) throws ParseException, IOException {
		DiskBasedPerfTest test = new DiskBasedPerfTest();
		test.loadQuery();
		File oDir = new File("/home/xiafan/KuaiPan/dataset/weiboexpr/2015_12_03/raw/");
		if (!oDir.exists())
			oDir.mkdirs();
		String[] confDirs = new String[] { "./conf/index_weibo_intern.conf", "./conf/index_weibo_lsmi.conf" };

		int i = 1;

		for (String conf : confDirs) {
			logger.info("load " + conf);
			File oFile = new File(oDir, new File(conf).getName().replace("properties", "txt"));
			test.test(conf, oFile);
		}

	}

}
