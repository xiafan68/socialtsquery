package expr;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.collections.Factory;

import collection.DefaultedPutMap;
import common.MidSegment;
import io.DirLineReader;
import searchapi.QueryType;
import segmentation.Segment;
import shingle.ITextShingle;
import shingle.ShingleFactory;
import util.Configuration;
import util.Pair;
import weibo.Tweet;

/**
 * 用于生成查询负载的类
 * 
 * @author xiafan
 *
 */
public class WorkLoadGen {

	BlockingQueue<WorkLoad> jobQueue = new LinkedBlockingQueue<WorkLoad>();
	QueryGen gen;
	float[] fractions;
	Random toss = new Random();
	ITextShingle shingle = ShingleFactory.createShingle();
	DirLineReader reader = null;
	Configuration conf;
	final float ratio;
	AtomicInteger lastTimeStamp = new AtomicInteger(0);

	public WorkLoadGen(float[] fractions) {
		this.fractions = fractions;
		ratio = fractions[1] / (fractions[0] + fractions[1]);
	}

	public void open(String confFile, String updateFile, String seedFile) {
		try {
			gen = new QueryGen(50);
			gen.loadQueryWithTime(seedFile);
			conf = new Configuration();
			conf.load(confFile);
			reader = new DirLineReader(updateFile);
		} catch (IOException e) {
			return;
		}
	}

	public boolean hasNext() {
		return reader.hasNext();
	}

	public void genNextJob() {
		float frac = toss.nextFloat();
		if (frac < fractions[0]) {
			// insert job
			String line = null;
			while (null != (line = reader.readLine())) {
				jobQueue.offer(new WorkLoad(WorkLoad.INSERT_JOB, parseTweetSegs(line)));
				break;
			}
		} else if (frac < fractions[0] + fractions[1]) {
			// realtime query
			String word = wordQueue.poll();
			if (word != null) {
				int start = lastTimeStamp.get();
				jobQueue.offer(new WorkLoad(WorkLoad.QUEYR_JOB, new TKQuery(Arrays.asList(word), 50,
						start - conf.queryStartTime(), start - conf.queryStartTime() + 12, QueryType.WEIGHTED)));
			}
		} else {
			// historical query
			Pair<List<String>, Integer> seed = gen.nextQuery();
			jobQueue.offer(new WorkLoad(WorkLoad.QUEYR_JOB,
					new TKQuery(seed.getKey(), 50, seed.getValue() - conf.queryStartTime(),
							seed.getValue() - conf.queryStartTime() + 24 * 2 * 30, QueryType.WEIGHTED)));
		}
	}

	public static class TKQuery {
		public List<String> words;
		public int topk;
		public int start;
		public int end;
		public QueryType type;

		public TKQuery(List<String> words, int topk, int start, int end, QueryType type) {
			super();
			this.words = words;
			this.topk = topk;
			this.start = start;
			this.end = end;
			this.type = type;
		}

		@Override
		public String toString() {
			return "TKQuery [words=" + words + ", topk=" + topk + ", start=" + start + ", end=" + end + ", type=" + type
					+ "]";
		}

	}

	int count = 0;

	Map<String, Integer> counters = DefaultedPutMap.decorate(new HashMap<String, Integer>(), new Factory() {
		@Override
		public Object create() {
			return 0;
		}
	});

	private BlockingQueue<String> wordQueue = new LinkedBlockingQueue<String>();

	private static final int SAMPLE_INTERVAL = 10000;

	private void updatePopWords(List<String> words) {
		for (String word : words) {
			counters.put(word, counters.get(word) + 1);
		}

		if (count % SAMPLE_INTERVAL == 0) {
			ArrayList<Entry<String, Integer>> entries = new ArrayList<Entry<String, Integer>>(counters.entrySet());
			Collections.sort(entries, new Comparator<Entry<String, Integer>>() {
				@Override
				public int compare(Entry<String, Integer> o1, Entry<String, Integer> o2) {
					return o1.getValue().compareTo(o2.getValue());
				}
			});
			for (int i = 0; i < SAMPLE_INTERVAL * ratio && i < entries.size(); i++) {
				wordQueue.add(entries.get(i).getKey());
			}
			counters.clear();
		}
	}

	private Pair<List<String>, MidSegment> parseTweetSegs(String line) {
		int idx = line.lastIndexOf('\t');
		if (idx < 0) {
			return null;
		}

		String tweetField = line.substring(0, idx);
		String histField = line.substring(idx + 1);
		Segment seg = new Segment();
		seg.parse(histField);
		lastTimeStamp.set(seg.getStart());

		seg.setStart(seg.getStart() - conf.queryStartTime());
		seg.setEndTime(seg.getEndTime() - conf.queryStartTime());
		Tweet tweet = new Tweet();
		tweet.parse(tweetField);
		long mid = -1;
		try {
			mid = Long.parseLong(tweet.getMid());
		} catch (Exception ex) {
		}

		try {
			List<String> words = shingle.shingling(tweet.getContent(), false);
			updatePopWords(words);
			return new Pair<List<String>, MidSegment>(words, new MidSegment(mid, seg));
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}
	}

	public BlockingQueue<WorkLoad> getJobQueue() {
		return jobQueue;
	}

	public static class WorkLoad {
		public WorkLoad(int jobType, Object data) {
			this.type = jobType;
			this.data = data;
		}

		public static final int INSERT_JOB = 0;
		public static final int QUEYR_JOB = 1;
		public int type;
		public Object data;

		@Override
		public String toString() {
			return "WorkLoad [type=" + type + ", data=" + data + "]";
		}
	}

}
