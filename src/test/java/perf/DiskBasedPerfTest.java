package perf;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.collections.Factory;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import core.commom.TempKeywordQuery;
import core.executor.IQueryExecutor;
import core.executor.WeightedQueryExecutor;
import core.lsmo.OctreeBasedLSMTFactory;
import core.lsmt.LSMTInvertedIndex;
import expr.QueryGen;
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

	QueryGen gen = new QueryGen(20);

	public DiskBasedPerfTest() {

	}

	public void loadQuery(String querySeed) throws IOException {
		gen.loadQueryWithTime(querySeed);
	}

	LSMTInvertedIndex indexReader;
	IQueryExecutor indexExec;
	public boolean multiPart = false;

	public void loadIndex(Path dir) throws IOException {
		if (indexReader != null) {
			indexReader.close();
		}
		PropertyConfigurator.configure("conf/log4j-server2.properties");
		Configuration conf = new Configuration();
		conf.load("conf/index_twitter.conf");
		LSMTInvertedIndex indexReader = new LSMTInvertedIndex(conf);
		/*
		 * indexReader.addPartition(new PartitionMeta(31), dir, new
		 * Configuration()); indexExec = new PartitionExecutor(indexReader);
		 * indexExec.setMaxLifeTime((int) Math.pow(2, 31));
		 */

		newExec();
	}

	private void newExec() {
		if (multiPart) {
			//indexExec = new MultiPartitionExecutor(indexReader);
		} else {
			indexExec = new WeightedQueryExecutor(indexReader);
			indexExec.setMaxLifeTime((int) Math.pow(2, 17));
		}
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
				// logger.info(k + " " + start + " " + (start + width) + " "
				// + keywords);

				Interval window = new Interval(1, start, start + width, 1);
				TempKeywordQuery tQuery = new TempKeywordQuery(query.arg0.toArray(new String[query.arg0.size()]),
						window, k);
				// Profile.instance.start("query");
				newExec();
				Profile.instance.start(Profile.TOTAL_TIME);
				indexExec.query(tQuery);
				Iterator<Interval> res = indexExec.getAnswer();
				Profile.instance.end(Profile.TOTAL_TIME);
				/*
				 * int count = 0; while (res.hasNext()) { res.next(); count++; }
				 * logger.info(tQuery + ":" + count);
				 */
			} catch (Exception ex) {
				logger.error(k + " " + start + " " + (start + width) + " " + keywords.size());
			}
			JSONObject perf = Profile.instance.toJSON();

			// System.out.println(String.format(
			// "keywords %s,start:%d, end:%d; perf:%s",
			// keywords.toString(), start, start + width, perf));
			// System.out.println(keywords + " time:" + perf);
			updateProfile(map, perf);
			// logger.info(JSONObject.fromObject(map));
			Profile.instance.reset();
			// Runtime.getRuntime().exec(
			// "sync && echo 1 > /proc/sys/vm/drop_caches");
		}
		System.gc();
		return counter;
	}

	public void selectResult(String conf, String oFile) throws ParseException, IOException {
		loadIndex(new Path(conf));
		System.setOut(new PrintStream(new FileOutputStream("/home/xiafan/temp/" + oFile)));

		for (int offset : offsets) {
			for (int width : widths) {
				for (int k : ks) {
					logger.info(String.format("offset %d, width %d; k is %d", offset, width, k));

					gen.resetCur();
					while (gen.hasNext()) {
						Pair<List<String>, Integer> query = gen.nextQuery();
						List<String> keywords = query.arg0;
						int start = query.arg1 + offset;

						Interval window = new Interval(1, start, start + width, 1);
						TempKeywordQuery tQuery = new TempKeywordQuery(
								query.arg0.toArray(new String[query.arg0.size()]), window, k);
						Profile.instance.start("query");
						newExec();
						indexExec.query(tQuery);
						Iterator<Interval> res = indexExec.getAnswer();
						logger.info("query " + k + " " + start + " " + (start + width) + " "
								+ StringUtils.join(keywords, " "));

						while (res.hasNext())
							System.out.println(res.next());
						System.out.flush();
						/*
						 * System.out .println(String .format(
						 * "keywords %s,start:%d, end:%d; len is %s; least score is %f"
						 * , keywords.toString(), start, start + width,
						 * invs.size(), invs.get(0).getAggValue()));
						 */
					}
				}
			} // end of widths
		}
		System.setOut(System.out);
	}

	public void test(String conf, String oDir, String format) throws ParseException, IOException {
		File dirFile = new File(oDir);
		if (!dirFile.exists())
			dirFile.mkdirs();
		loadIndex(new Path(conf));
		// testOneRound(0, 100000, 100);

		for (int offset : offsets) {
			File logFile = new File(oDir, String.format(format, offset));
			if (logFile.exists())
				logFile.delete();
			OutputStream os = StreamUtils.outputStream(logFile);
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
			os.close();
		}
	}

	private static double getValue(JSONObject obj) {
		return obj.getDouble("totalTime") / obj.getDouble("count");
	}

	static final String[] COUNTER_FIELDS = new String[] { Profile.DATA_BLOCK, Profile.META_BLOCK, Profile.SKIPPED_BLOCK,
			Profile.TOPK, Profile.CAND, Profile.WASTED_REC, Profile.TWASTED_REC };
	static final String[] IOTimeFields = new String[] { Profile.toTimeTag(Profile.DATA_BLOCK), Profile.TOTAL_TIME,
			Profile.UPDATE_STATE };

	/**
	 * 将新测试的代价更新到map中去
	 * 
	 * @param map
	 * @param perf
	 */
	public static void updateProfile(DefaultedPutMap<String, Double> map, JSONObject perf) {
		JSONObject cur = perf.getJSONObject("perf");
		JSONObject obj = null;

		for (String field : IOTimeFields) {
			obj = cur.getJSONObject(field);
			double time = 0.0;
			if (obj.containsKey("totalTime"))
				time = obj.getDouble("totalTime");
			map.put(field, map.get(field) + time);
		}

		cur = perf.getJSONObject("ecounters");
		for (String field : COUNTER_FIELDS) {
			double time = 0.0;
			if (cur.containsKey(field)) {
				time = cur.getDouble(field);
			}
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
		return ret;
	}

	/**
	 * -i /Users/xiafan/temp/output -o
	 * /Users/xiafan/快盘/dataset/exprresult/2015_3_21/raw/ -s
	 * /Users/xiafan/快盘/dataset/time_series/nqueryseed.txt
	 * 
	 * @param args
	 * @throws ParseException
	 * @throws IOException
	 */
	public static void main(String[] args) throws ParseException, IOException {
		Options options = new Options();
		options.addOption("s", true, "path of query seed file");
		options.addOption("o", true, "output directory");
		options.addOption("i", true, "index directory");

		CommandLineParser parser = new PosixParser();
		HelpFormatter formatter = new HelpFormatter();
		CommandLine cmd = null;
		try {
			cmd = parser.parse(options, args);
		} catch (org.apache.commons.cli.ParseException e) {
			formatter.printHelp("", options);
			return;
		}

		String querySeed = "/home/xiafan/KuaiPan/dataset/time_series/nqueryseed.txt";
		if (cmd.hasOption("s")) {
			querySeed = cmd.getOptionValue("s");
		}
		// querySeed = "/Users/xiafan/快盘/dataset/time_series/nqueryseed.txt";

		String oDir = "/home/xiafan/KuaiPan/dataset/exprresult/2015_3_24/raw/";
		if (cmd.hasOption("o")) {
			oDir = cmd.getOptionValue("o");
			System.out.println(oDir);
		}

		String iDir = "/home/xiafan/temp/invindex_parts";
		if (cmd.hasOption("o")) {
			iDir = cmd.getOptionValue("i");
		}
		String cmdLine = "-i -o -s";

		// oDir = "/Users/xiafan/快盘/dataset/exprresult/2015_3_21/raw/";

		DiskBasedPerfTest test = new DiskBasedPerfTest();
		test.loadQuery(querySeed);

		String[] formats = new String[] { "invindex_o%dw.txt", "minvindex_o%dw.txt" };
		// test.testAllKeywords(conf, oDir + formats[i++], 0, 12, 50);
		String[] iDirs = new String[] { "/home/xiafan/文档/dataset/output", "/home/xiafan/temp/invindex_parts" };
		// test.multiPart = true;
		for (int i = 0; i < formats.length; i++) {
			test.test(iDirs[i], oDir, formats[i]);
			test.multiPart = !test.multiPart;
		}
	}
}
