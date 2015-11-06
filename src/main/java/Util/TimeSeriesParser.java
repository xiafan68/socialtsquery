package Util;

import java.io.DataOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import common.Tweet;
import fanxia.file.DirLineReader;
import fanxia.file.FileUtil;
import xiafan.util.Histogram;

public class TimeSeriesParser {
	public static Tweet parseSeries(String line) {
		int idx = line.lastIndexOf('\t');
		if (idx < 0) {
			return null;
		}
		Histogram hist = new Histogram();

		String tweetField = line.substring(0, idx);
		String histField = line.substring(idx + 1);
		Tweet tweet = new Tweet();
		tweet.parse(tweetField);
		hist.fromString(histField);
		tweet.setRtCount(hist.total());
		return tweet;
	}

	public static void sortFileByRtCount(String input, String output) throws IOException {
		DirLineReader reader = null;
		try {
			reader = new DirLineReader(input);
		} catch (IOException e) {
			return;
		}
		String line = null;
		int i = 0;
		long start = System.currentTimeMillis();
		List<Tweet> tweet = new ArrayList<Tweet>();
		while (null != (line = reader.readLine())) {
			Tweet cur = parseSeries(line);
			if (cur != null)
				tweet.add(cur);
		}

		reader.close();

		Collections.sort(tweet, new Comparator<Tweet>() {
			@Override
			public int compare(Tweet o1, Tweet o2) {
				return 0 - Integer.compare(o1.getRtCount(), o2.getRtCount());
			}

		});
		DataOutputStream oStream = FileUtil.openDos(output);
		for (Tweet t : tweet)
			oStream.write((t.toString() + "\n").getBytes());
		oStream.close();
	}

	public static void main(String[] args) throws IOException {
		sortFileByRtCount("/Users/kc/Documents/dataset/twitter/part-00000",
				"/Users/kc/Documents/dataset/twitter/spart-00000");
	}
}
