package extract;

import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;

import segmentation.Segment;
import xiafan.file.DirLineReader;
import xiafan.util.Pair;
import xiafan.util.Triple;
import core.index.MidSegment;
import extract.common.MRDataFormat;

/**
 * 计算一个term对应的所有的Segmented item在每个时间点的最大转发数序列
 * @author xiafan
 *
 */
public class TermMaxScoreExtraction {
	String input;
	String output;

	private TermMaxScoreExtraction(String input, String output) {
		super();
		this.input = input;
		this.output = output;
	}

	public Pair<Integer, List<Float>> genMaxSeries(List<MidSegment> segs) {
		Collections.sort(segs, new Comparator<MidSegment>() {
			/**
			 * 先按照start时间升序排列，后按照id排序
			 * @param o1
			 * @param o2
			 * @return
			 */
			public int compare(MidSegment o1, MidSegment o2) {
				int ret = Integer.compare(o1.getStart(), o2.getStart());
				if (ret == 0) {
					ret = Integer.compare(o1.getEndTime(), o2.getEndTime());
					if (ret == 0) {
						ret = Long.compare(o1.mid, o2.mid);
					}
				}
				return ret;
			}
		});
		List<Float> scoreSeries = new ArrayList<Float>();
		Pair<Integer, List<Float>> ret = new Pair<Integer, List<Float>>(segs
				.get(0).getStart(), scoreSeries);

		for (MidSegment mSeg : segs) {
			for (int i = mSeg.getStart(); i <= mSeg.getEndTime(); i++) {
				int pos = i - ret.arg0;
				if (pos >= scoreSeries.size()) {
					while (scoreSeries.size() < pos + 1)
						scoreSeries.add(0f);
				}
				float value = mSeg.getValue(i);
				if (value > scoreSeries.get(pos))
					scoreSeries.set(pos, value);
			}
		}
		return ret;
	}

	public void transform() throws IOException {
		DirLineReader reader = new DirLineReader(input);
		String line = null;
		HashSet<Long> mids = new HashSet<Long>();
		while (null != (line = reader.readLine())) {
			Triple<Long, String, Segment> triple = MRDataFormat
					.parseSegment(line);
			mids.add(triple.arg0);
		}
		System.out.println("mids loaded");

		List<MidSegment> segs = new ArrayList<MidSegment>();
		reader = new DirLineReader(
				"/Users/xiafan/Documents/dataset/seg_paper_expr/dataset/event_segs");
		while (null != (line = reader.readLine())) {
			String[] fields = line.split("\t");
			long mid = Long.parseLong(fields[0]);
			Segment seg = new Segment();
			seg.parse(fields[1]);
			if (mids.contains(mid)) {
				segs.add(new MidSegment(mid, seg));
			}
		}
		System.out.println("mid segment loaded");

		Pair<Integer, List<Float>> series = genMaxSeries(segs);
		write(series);
	}

	public void run() throws IOException {
		DirLineReader reader = new DirLineReader(input);
		String line = null;
		List<MidSegment> segs = new ArrayList<MidSegment>();
		while (null != (line = reader.readLine())) {
			Triple<Long, String, Segment> triple = MRDataFormat
					.parseSegment(line);
			segs.add(new MidSegment(triple.arg0, triple.arg2));
		}
		reader.close();

		Pair<Integer, List<Float>> series = genMaxSeries(segs);
		write(series);
	}

	private void write(Pair<Integer, List<Float>> series) throws IOException {
		DataOutputStream dos = new DataOutputStream(
				new FileOutputStream(output));
		int i = 0;
		for (Float value : series.arg1) {
			dos.write(String.format("%d\t%f\n", series.arg0 + (i++), value)
					.getBytes());
		}
		dos.close();
	}

	public static void main(String[] args) throws IOException {
		TermMaxScoreExtraction ext = new TermMaxScoreExtraction(
				"/Users/xiafan/快盘/dataset/time_series/beijing.txt",
				"/Users/xiafan/快盘/dataset/time_series/beijing_score.txt");
		// ext.run();
		ext.transform();
	}
}
