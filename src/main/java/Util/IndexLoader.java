package Util;

import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.Map.Entry;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import common.MidSegment;
import common.Tweet;
import core.lsmt.LSMOInvertedIndex;
import fanxia.file.DirLineReader;
import segmentation.ISegmentation.ISegSubscriber;
import segmentation.Interval;
import segmentation.SWSegmentation;
import segmentation.Segment;
import shingle.TextShingle;
import xiafan.util.Histogram;

public class IndexLoader {
	private static Logger logger = Logger.getLogger(IndexLoader.class);

	private static void loadSegs(String[] args) throws IOException {
		PropertyConfigurator.configure("conf/log4j-server.properties");
		Configuration conf = new Configuration();
		conf.load("conf/index.conf");

		LSMOInvertedIndex index = new LSMOInvertedIndex(conf);
		try {
			index.init();
		} catch (IOException e) {
			e.printStackTrace();
			index = null;
		}

		// "/Users/xiafan/Documents/dataset/expr/twitter/twitter_segs"
		DirLineReader reader = new DirLineReader(args[0]);
		String line = null;
		int i = 0;
		while (null != (line = reader.readLine())) {
			MidSegment seg = new MidSegment();
			seg.parse(line);
			index.insert(Long.toString(seg.getMid()), seg);
			if (i++ % 10 == 0) {
				logger.info("loaded " + i);
			}
			if (i > 1000)
				break;
		}
		index.close();
		reader.close();
	}

	private static void loadTweetsHist(String[] args) throws IOException {
		PropertyConfigurator.configure("conf/log4j-server.properties");
		Configuration conf = new Configuration();
		conf.load("conf/index.conf");

		final LSMOInvertedIndex index = new LSMOInvertedIndex(conf);
		try {
			index.init();
		} catch (IOException e) {
			e.printStackTrace();
			return;
		}
		TextShingle shingle = new TextShingle(null);
		// "/Users/xiafan/Documents/dataset/expr/twitter/twitter_segs"
		DirLineReader reader = new DirLineReader(args[0]);
		String line = null;
		int i = 0;
		long start = System.currentTimeMillis();
		while (null != (line = reader.readLine())) {
			int idx = line.lastIndexOf('\t');
			if (idx < 0) {
				return;
			}
			Histogram hist = new Histogram();

			String tweetField = line.substring(0, idx);
			String histField = line.substring(idx + 1);
			Tweet tweet = new Tweet();
			tweet.parse(tweetField);
			hist.fromString(histField);
			long midTmp = -1;
			try {
				midTmp = Long.parseLong(tweet.getMid());
			} catch (Exception ex) {
				UUID uid = UUID.randomUUID();
				midTmp = uid.getLeastSignificantBits() & uid.getMostSignificantBits();
			}
			final long mid = midTmp;
			final List<String> words = shingle.shingling(tweet.getContent());
			SWSegmentation seg = new SWSegmentation(mid, 5, null, new ISegSubscriber() {
				@Override
				public void newSeg(Interval preInv, Segment seg) {
					try {
						index.insert(words, new MidSegment(mid, seg));
					} catch (IOException e) {
					}
				}
			});
			Iterator<Entry<Double, Integer>> iter = hist.groupby(1000 * 60 * 30).iterator();
			while (iter.hasNext()) {
				Entry<Double, Integer> entry = iter.next();
				seg.advance(entry.getKey().intValue(), entry.getValue());
			}
			seg.finish();
			if (++i % 1000 == 0) {
				long time = System.currentTimeMillis() - start;
				logger.info("inserting " + i + " items costs " + time + " ms, " + " average " + ((double) time / i)
						+ " ms/i");
			}
		}
		index.close();
		reader.close();
	}

	private static void loadTweetsSegs(String[] args) throws IOException {
		PropertyConfigurator.configure("conf/log4j-server.properties");
		Configuration conf = new Configuration();
		conf.load("conf/index.conf");

		final LSMOInvertedIndex index = new LSMOInvertedIndex(conf);
		try {
			index.init();
		} catch (IOException e) {
			e.printStackTrace();
			return;
		}
		TextShingle shingle = new TextShingle(null);
		// "/Users/xiafan/Documents/dataset/expr/twitter/twitter_segs"
		DirLineReader reader = new DirLineReader(args[0]);
		String line = null;
		int i = 0;
		long start = System.currentTimeMillis();
		Map<String, Long> mapping = new HashMap<String, Long>();
		while (null != (line = reader.readLine())) {
			int idx = line.lastIndexOf('\t');
			if (idx < 0) {
				return;
			}
			String tweetField = line.substring(0, idx);
			String histField = line.substring(idx + 1);
			Segment seg = new Segment();
			seg.parse(histField);
			Tweet tweet = new Tweet();
			tweet.parse(tweetField);
			long midTmp = -1;
			try {
				midTmp = Long.parseLong(tweet.getMid());
			} catch (Exception ex) {
				// logger.error(ex.getMessage());
				if (mapping.containsKey(tweet.getMid())) {
					midTmp = mapping.get(tweet.getMid());
				} else {
					midTmp = 0 - mapping.size();
					mapping.put(tweet.getMid(), midTmp);
				}
			}
			final long mid = midTmp;
			final List<String> words = shingle.shingling(tweet.getContent());
			index.insert(words, new MidSegment(midTmp, seg));
			if (i++ % 1000 == 0) {
				long time = System.currentTimeMillis() - start;
				logger.info("inserting " + i + " items costs " + time + " ms, " + " average " + ((double) time / i)
						+ " ms/i");
			}
			if (Runtime.getRuntime().freeMemory() < 1024 * 1024 * 100)
				break;
		}
		DataOutputStream output = new DataOutputStream(new FileOutputStream("/tmp/mapping.txt"));
		for (Entry<String, Long> entry : mapping.entrySet()) {
			output.writeUTF(entry.getKey());
			output.writeLong(entry.getValue());
		}
		output.close();
		index.close();
		reader.close();
	}

	public static void main(String[] args) throws IOException {
		loadTweetsHist(args);
		// loadTweetsSegs(args);
	}
}
