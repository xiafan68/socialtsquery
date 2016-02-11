package expr;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.collections.Factory;

import collection.DefaultedPutMap;
import common.MidSegment;
import expr.WorkLoadGen.WorkLoad;
import io.DirLineReader;
import segmentation.Segment;
import shingle.ITextShingle;
import shingle.ShingleFactory;
import util.Configuration;
import util.Pair;
import weibo.Tweet;

/**
 * 生成倒排索引插入流
 * 
 * @author xiafan
 *
 */
public class InsertStreamGen {
	private static final int SAMPLE_INTERVAL = 10000;

	ITextShingle shingle = ShingleFactory.createShingle();
	private Configuration conf;
	private DirLineReader reader;
	int lastTimeStamp = 0;
	Subscriber sub;

	public static interface Subscriber {
		public void onPopWordsChanged(List<Entry<String, Integer>> popWords, long time);
	}

	public InsertStreamGen() {

	}

	public void setSub(Subscriber sub) {
		this.sub = sub;
	}

	public void init(String confFile, String updateFile) {
		try {
			conf = new Configuration();
			conf.load(confFile);
			reader = new DirLineReader(updateFile);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public boolean hasNext() {
		return reader.hasNext();
	}

	/**
	 * @thread_unsafe
	 * @return
	 */
	public WorkLoad genNextJob() {
		WorkLoad ret = null;
		// insert job
		String line = null;
		while (null != (line = reader.readLine())) {
			ret = new WorkLoad(WorkLoad.INSERT_JOB, parseTweetSegs(line));
			break;
		}
		return ret;
	}

	int count = 0;
	Map<String, Integer> counters = DefaultedPutMap.decorate(new HashMap<String, Integer>(), new Factory() {
		@Override
		public Object create() {
			return 0;
		}
	});

	private Pair<List<String>, MidSegment> parseTweetSegs(String line) {
		int idx = line.lastIndexOf('\t');
		if (idx < 0) {
			return null;
		}

		String tweetField = line.substring(0, idx);
		String histField = line.substring(idx + 1);
		Segment seg = new Segment();
		seg.parse(histField);
		lastTimeStamp = seg.getStart();

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

	private void updatePopWords(List<String> words) {
		for (String word : words) {
			counters.put(word, counters.get(word) + 1);
		}

		if (count % SAMPLE_INTERVAL == 0) {
			List<Entry<String, Integer>> entries = new ArrayList<Entry<String, Integer>>(counters.entrySet());
			Collections.sort(entries, new Comparator<Entry<String, Integer>>() {
				@Override
				public int compare(Entry<String, Integer> o1, Entry<String, Integer> o2) {
					return o1.getValue().compareTo(o2.getValue());
				}
			});
			counters.clear();
			entries = entries.subList(0, Math.min(100, counters.size()));
			if (sub != null) {
				sub.onPopWordsChanged(entries, lastTimeStamp);
			}
		}
	}
}
