package extract;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import segmentation.ISegmentation.ISegSubscriber;
import segmentation.Interval;
import segmentation.MaxHistogram;
import segmentation.SWSegmentation;
import segmentation.Segment;
import xiafan.util.Histogram;
import common.Tweet;
import extract.common.MRDataFormat;

public class TweetAggValueJob {
	public static class SegmentationMapper extends
			Mapper<LongWritable, Text, LongWritable, Text> {
		Text outValue = new Text();
		private static int granu = 1000 * 60 * 30;// 30min

		@Override
		public void map(LongWritable key, Text value, Context ctx)
				throws IOException {
			Histogram hist = new Histogram();
			String row = value.toString();
			int idx = row.lastIndexOf('\t');
			if (idx < 0) {
				return;
			}
			try {
				String tweetField = row.substring(0, idx);
				String histField = row.substring(idx + 1);
				Tweet tweet = new Tweet();
				tweet.parse(tweetField);
				hist.fromString(histField);
				long mid = key.get();
				try {
					mid = Long.parseLong(tweet.getMid());
				} catch (Exception ex) {

				}
				final String content = tweet.getContent();
				SegCollector col = new SegCollector();
				SWSegmentation seg = new SWSegmentation(mid, 5, null, col);
				Iterator<Entry<Double, Integer>> iter = hist.groupby(granu)
						.iterator();
				while (iter.hasNext()) {
					Entry<Double, Integer> entry = iter.next();
					seg.advance(entry.getKey().intValue(), entry.getValue());
				}
				seg.finish();

				MaxHistogram sketch = MaxHistogram.constructByPoints(col.segs);
				// MaxHistogram sketch = new MaxHistogram();
				if (sketch != null)
					ctx.getCounter("sketch", "sketch number").increment(1);
				Interval inv = new Interval(mid, col.start(), col.end(),
						col.totalValue(), sketch);
				outValue.set(MRDataFormat.writeAgg(content, inv));
				ctx.write(new LongWritable(mid), outValue);
			} catch (Exception ex) {
				ex.printStackTrace();
				ctx.getCounter("sketch", ex.getMessage()).increment(1);
			}
		}
	}

	public static class SegCollector implements ISegSubscriber {
		public Interval lastInv = null;
		// public Segment lastSeg = null;
		public List<Segment> segs = new ArrayList<Segment>();

		public void newSeg(Interval preInv, Segment seg) {
			lastInv = preInv;
			// lastSeg = seg;
			segs.add(seg);
		}

		public int start() {
			if (lastInv != null)
				return lastInv.getStart();
			else if (segs.size() > 0)
				return segs.get(0).getStart();
			else
				return 0;
		}

		public int end() {
			if (segs.size() > 0)
				return segs.get(segs.size() - 1).getEndTime();
			else
				return 0;
		}

		public float totalValue() {
			if (segs.size() > 0) {
				Segment last = segs.get(segs.size() - 1);
				if (lastInv != null) {
					return lastInv.getAggValue() + last.getValue()
							- last.getStartCount();
				} else {
					return last.getValue();
				}
			}
			return 0.0f;
		}
	}

	public static class SegmentationReducer extends
			Reducer<LongWritable, Text, LongWritable, Text> {
		Text outKey = new Text();

		@Override
		public void reduce(LongWritable key, Iterable<Text> values, Context ctx)
				throws IOException, InterruptedException {
			Iterator<Text> iter = values.iterator();
			while (iter.hasNext())
				ctx.write(key, iter.next());
		}
	}

	public static void run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = new Job(conf);
		job.setJobName("Segmentation");
		job.setJarByClass(TweetAggValueJob.class);
		conf.setInt("mapred.task.timeout", 6000000);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(SegmentationMapper.class);
		job.setReducerClass(SegmentationReducer.class);

		job.setNumReduceTasks(30);
		TextInputFormat.setInputPaths(job, new Path(args[0]));
		Path output = new Path(args[1]);
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(output)) {
			fs.delete(output, true);
		}
		TextOutputFormat.setOutputPath(job, output);
		job.waitForCompletion(true);
	}

	// inputDir(the histogram), outDir1
	public static void main(String[] args) throws Exception {
		run(args);
	}
}
