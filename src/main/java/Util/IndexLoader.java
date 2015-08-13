package Util;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

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
			if (i++ % 1000 == 0) {
				System.out.println(i);
			}
			if (i > 10000)
				break;
		}
		index.close();
		reader.close();
	}

	private static void loadTweets(String[] args) throws IOException {
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
		}
		index.close();
		reader.close();
	}

	public static void main(String[] args) throws IOException {
		loadTweets(args);
	}
}
