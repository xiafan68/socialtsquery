package extract;

import java.io.IOException;
import java.util.Iterator;
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
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import segmentation.ISegmentation.ISegSubscriber;
import segmentation.Interval;
import segmentation.SWSegmentation;
import segmentation.Segment;
import xiafan.util.Histogram;
import xiafan.util.Pair;

import common.Tweet;

import extract.common.MRDataFormat;

/**
 * 将每条微博的转发序列进行segmentation,同时按照每个tweet的生命周期对数据进行分割
 * 
 * @author xiafan
 * 
 */
public class RtSeriesSegmentationJob {
	public static class SegmentationMapper extends
			Mapper<LongWritable, Text, LongWritable, Text> {

		public void map(LongWritable key, Text value, Context ctx)
				throws IOException, InterruptedException {
			ctx.write(key, value);
		}
	}

	public static class SegmentationReducer extends
			Reducer<LongWritable, Text, Text, Text> {
		Text outKey = new Text();
		Text outValue = new Text();
		private static int granu = 1000 * 60 * 30;// 30min
		private MultipleOutputs<Text, Text> mop;

		@Override
		public void setup(Context ctx) {
			mop = new MultipleOutputs<Text, Text>(ctx);
		}

		@Override
		public void cleanup(Context ctx) throws IOException,
				InterruptedException {
			mop.close();
		}

		public void reduce(LongWritable key, Iterable<Text> values,
				final Context ctx) throws IOException {
			Iterator<Text> vIter = values.iterator();
			while (vIter.hasNext()) {
				Text value = vIter.next();
				try {
					final Pair<Tweet, Histogram> data = MRDataFormat
							.parseTimeSeries(value.toString());
					if (data == null)
						continue;
					long midTmp = key.get();
					try {
						midTmp = Long.parseLong(data.arg0.getMid());
					} catch (Exception ex) {
					}

					final long mid = midTmp;

					final String part = "part"
							+ LifeTimePartitioner.part(data.arg1.width());
					SWSegmentation seg = new SWSegmentation(mid, 5, null,
							new ISegSubscriber() {
								public void newSeg(Interval preInv, Segment seg) {
									// outValue.set(content + "|#|" +
									// seg.toString());
									outValue.set(seg.toString());
									try {
										mop.write(part,
												new Text(data.arg0.toString()),
												outValue);
									} catch (IOException e) {
									} catch (InterruptedException e) {
										e.printStackTrace();
									}
								}
							});
					Iterator<Entry<Double, Integer>> iter = data.arg1.groupby(
							granu).iterator();
					while (iter.hasNext()) {
						Entry<Double, Integer> entry = iter.next();
						seg.advance(entry.getKey().intValue(), entry.getValue());
					}
					seg.finish();
				} catch (Exception ex) {

				}
			}
		}
	}

	public static void run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = new Job(conf);
		job.setJobName("Segmentation");

		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(SegmentationMapper.class);
		job.setReducerClass(SegmentationReducer.class);

		job.setNumReduceTasks(10);
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
