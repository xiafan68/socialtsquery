package expr;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;

import org.apache.commons.collections.Factory;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import collection.DefaultedPutMap;
import core.lsmt.LSMTInvertedIndex;
import io.StreamLogUtils;
import io.StreamUtils;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import net.sf.json.JSONObject;
import util.Configuration;
import util.Pair;
import util.Profile;
import util.ProfileField;

public class DiskBasedPerfTest {
	private static final Logger logger = Logger.getLogger(DiskBasedPerfTest.class);
	QueryGen gen = new QueryGen(10);
	LSMTInvertedIndex index;

	public DiskBasedPerfTest() {
	}

	public void loadQuery(String seed) throws IOException {
		//
		// String seed =
		// "/home/xiafan/KuaiPan/dataset/time_series/nqueryseed.txt";
		// "/home/xiafan/dataset/twitter/tqueryseed.txt"
		gen.loadQueryWithTime(seed);
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

	public static final String[] queryTypes = new String[] { "WEIGHTED", "AND", "OR" };
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
	private HashMap<String, Double> testOneRound(int offset, int width, int k, String queryType) throws IOException {
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
			List<String> keywords = query.getKey();
			logger.info(keywords.toString());
			logger.info(offset + " " + width + " " + k);
			int start = query.getValue() - curConf.queryStartTime() + offset;
			try {
				index.query(keywords, start, start + width, k, queryType);
			} catch (Exception ex) {
				logger.error(ex.toString() + "----" + k + " " + start + " " + (start + width) + " " + keywords.size());
			}
			JSONObject perf = Profile.instance.toJSON();

			updateProfile(map, perf);
			Profile.instance.reset();
			// Runtime.getRuntime().exec("sync && echo 1 >
			// /proc/sys/vm/drop_caches");
		}
		System.gc();
		return counter;
	}

	Configuration curConf = new Configuration();

	private List<String> testSettings(int[] ks, int[] offsets, int[] widths) throws IOException {
		List<String> logs = new ArrayList<String>();
		for (int offset : offsets)
			for (int k : ks)
				for (int width : widths)
					for (String queryType : queryTypes) {
						HashMap<String, Double> counter = testOneRound(offset, width, k, queryType);
						HashMap<String, Object> profile = new HashMap<String, Object>(normalize(counter, gen.size()));
						profile.put("width", width);
						profile.put("words", 2);
						profile.put("k", k);
						profile.put("offset", offset);
						profile.put("type", queryType);
						logger.info(JSONObject.fromObject(profile));
						logs.add(JSONObject.fromObject(profile).toString());
						try {
							Thread.sleep(100);
						} catch (InterruptedException e) {
						}
					}
		return logs;
	}

	public void testByDefault(String conf, File oFile) throws IOException {
		curConf.load(conf);
		index = load(conf);
		testOneRound(0, 100, 100, queryTypes[0]);
		File logFile = oFile;
		if (logFile.exists())
			logFile.delete();
		OutputStream os = StreamUtils.outputStream(logFile);

		for (int i = 0; i < 5; i++) {
			for (String log : testSettings(new int[] { 50 }, new int[] { 0 }, new int[] { 24 })) {
				StreamLogUtils.log(os, log);
			}
		}

		os.flush();
		os.close();
		index.close();
	}

	public void testByAllFacts(String conf, File oFile) throws IOException {
		curConf.load(conf);
		index = load(conf);
		testOneRound(0, 100, 100, queryTypes[0]);
		File logFile = oFile;
		if (logFile.exists())
			logFile.delete();
		OutputStream os = StreamUtils.outputStream(logFile);

		for (int i = 0; i < 5; i++) {
			for (String log : testSettings(ks, new int[] { 0 }, new int[] { 24 })) {
				StreamLogUtils.log(os, log);
			}

			for (String log : testSettings(new int[] { 50 }, new int[] { 0 }, widths)) {
				StreamLogUtils.log(os, log);
			}

			for (String log : testSettings(new int[] { 50 }, offsets, new int[] { 24 })) {
				StreamLogUtils.log(os, log);
			}
		}

		os.flush();
		os.close();
		index.close();
	}

	public void test(String conf, File oFile) throws ParseException, IOException {
		curConf.load(conf);
		index = load(conf);
		testOneRound(0, 100, 100, queryTypes[0]);
		File logFile = oFile;
		if (logFile.exists())
			logFile.delete();
		OutputStream os = StreamUtils.outputStream(logFile);
		for (String queryType : queryTypes)
			for (int offset : offsets) {
				List<String> logs = new ArrayList<String>();
				for (int width : widths) {
					for (int k : ks) {
						HashMap<String, Double> counter = testOneRound(offset, width, k, queryType);
						HashMap<String, Object> profile = new HashMap<String, Object>(normalize(counter, gen.size()));
						profile.put("width", width);
						profile.put("words", 2);
						profile.put("k", k);
						profile.put("offset", offset);
						profile.put("type", queryType);
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

	static final String[] pFields = new String[] { ProfileField.TOTAL_TIME.toString(),
			ProfileField.UPDATE_CAND.toString(), ProfileField.MAINTAIN_CAND.toString() };

	static final String[] candFields = new String[] { ProfileField.CAND.toString(),
			ProfileField.WASTED_REC.toString() };

	static final String[] IOFields = new String[] { ProfileField.NUM_BLOCK.toString() };

	static final String[] IOTimeFields = new String[] { ProfileField.READ_BLOCK.toString() };

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

	public static void main(String[] args) throws ParseException, IOException {
		OptionParser parser = new OptionParser();
		parser.accepts("e", "experiment directory").withRequiredArg().ofType(String.class);
		parser.accepts("c", "index configuration file").withRequiredArg().ofType(String.class);
		parser.accepts("q", "data file location").withRequiredArg().ofType(String.class);
		parser.accepts("s", "type:all, single, facts").withRequiredArg().ofType(String.class);

		OptionSet opts = null;
		try {
			opts = parser.parse(args);
		} catch (Exception exception) {
			parser.printHelpOn(System.out);
			return;
		}
		if (!opts.hasOptions()) {
			parser.printHelpOn(System.out);
			return;
		}

		DiskBasedPerfTest test = new DiskBasedPerfTest();
		test.loadQuery(opts.valueOf("q").toString());

		File oDir = new File(opts.valueOf("e").toString());
		if (!oDir.exists())
			oDir.mkdirs();

		String[] confDirs = new String[opts.valuesOf("c").size()];
		// new String[] { "./conf/index_weibo_intern.conf",
		// "./conf/index_weibo_lsmi.conf" };
		opts.valuesOf("c").toArray(confDirs);
		String ttype = (String) opts.valueOf("s");
		for (String conf : confDirs) {
			logger.info("load " + conf);
			File file = new File(conf);
			if (file.isDirectory())
				for (File curConf : file.listFiles()) {
					execTest(ttype, test, curConf, oDir);
				}
			else {
				execTest(ttype, test, file, oDir);
			}
		}
	}

	private static void execTest(String ttype, DiskBasedPerfTest test, File curConf, File oDir)
			throws ParseException, IOException {
		File oFile = new File(oDir, curConf.getName().replace("conf", "txt"));
		if (ttype.equals("all")) {
			test.test(curConf.getAbsolutePath(), oFile);
		} else if (ttype.equals("single")) {
			test.testByDefault(curConf.getAbsolutePath(), oFile);
		} else if (ttype.equals("facts")) {
			test.testByAllFacts(curConf.getAbsolutePath(), oFile);
		}
	}

}
