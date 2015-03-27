package extract.common;

import java.io.IOException;

import segmentation.Interval;
import segmentation.MaxHistogram;
import segmentation.Segment;
import xiafan.util.Histogram;
import xiafan.util.Pair;
import xiafan.util.Triple;

import common.Tweet;

public class MRDataFormat {
	public static Triple<Long, String, Segment> parseSegment(String row) {
		int idx = row.lastIndexOf("\t");
		if (idx > 0) {
			Long mid = Long.parseLong(row.substring(0, idx));
			row = row.substring(idx + "\t".length(), row.length());
			idx = row.indexOf("|#|");
			String content = row.substring(0, idx);
			Segment seg = new Segment();
			seg.parse(row.substring(idx + "|#|".length(), row.length()));
			return new Triple<Long, String, Segment>(mid, content, seg);
		}
		return null;
	}

	public static Pair<Tweet, Histogram> parseTimeSeries(String row) {
		int idx = row.lastIndexOf("\t");
		if (idx > 0) {
			Tweet t = new Tweet();
			t.parse(row.substring(0, idx));
			row = row.substring(idx + "\t".length(), row.length());
			Histogram hist = new Histogram();
			hist.fromString(row);
			return new Pair<Tweet, Histogram>(t, hist);
		}
		return null;
	}

	public static String writeAgg(String content, Interval data) {
		String hist = "";
		if (data.getHist() != null)
			hist = data.getHist().toString();
		String ret = String.format("%s|#|%d|#|%d|#|%f|#|%s", content,
				data.getStart(), data.getEnd(), data.getAggValue(), hist);
		return ret;
	}

	public static Pair<String, Interval> parseAgg(String row) {
		int idx = row.lastIndexOf("\t");
		if (idx > 0) {
			long mid = Long.parseLong(row.substring(0, idx));
			row = row.substring(idx + "\t".length(), row.length());
			idx = row.lastIndexOf("|#|");
			MaxHistogram hist = null;
			if (idx + 3 < row.length()) {
				hist = new MaxHistogram();
				try {
					hist.read(row.substring(idx + 3));
				} catch (IOException e) {
					e.printStackTrace();
				}
			}

			row = row.substring(0, idx);
			idx = row.lastIndexOf("|#|");
			float agg = Float.parseFloat(row.substring(idx + 3));
			row = row.substring(0, idx);
			idx = row.lastIndexOf("|#|");
			int end = Integer.parseInt(row.substring(idx + 3));
			row = row.substring(0, idx);
			idx = row.lastIndexOf("|#|");
			int start = Integer.parseInt(row.substring(idx + 3));
			row = row.substring(0, idx);

			return new Pair<String, Interval>(row, new Interval(mid, start,
					end, agg, hist));
		}
		return null;
	}
}
