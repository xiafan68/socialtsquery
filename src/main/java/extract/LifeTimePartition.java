package extract;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.Logger;

import xiafan.util.Histogram;
import xiafan.util.Pair;
import common.Tweet;
import core.mr.InvertedIndexBuilder;
import extract.common.MRDataFormat;

/**
 * 对时间序列按照lifetime进行partition
 * 
 * @author xiafan
 * 
 */
public class LifeTimePartition {
	private static Logger logger = Logger.getLogger(LifeTimePartition.class);

	public static class TimeSeriesMapper extends
			Mapper<LongWritable, Text, IntWritable, Text> {

		public void map(LongWritable key, Text value, Context ctx)
				throws IOException, InterruptedException {
			ctx.write(new IntWritable(value.hashCode()), value);
		}
	}

	public static class TimeSeriesReducer extends
			Reducer<IntWritable, Text, Text, NullWritable> {
		private MultipleOutputs<Text, NullWritable> mop;

		@Override
		public void setup(Context conf) {
			mop = new MultipleOutputs<Text, NullWritable>(conf);
		}

		@Override
		public void cleanup(Context ctx) throws IOException,
				InterruptedException {
			mop.close();
		}

		private int part(long width) {
			int i = 0;
			long tmp = width;
			while (tmp > 0) {
				tmp = tmp >> 1;
				if (tmp > 0)
					i++;
			}
			if (width - tmp >= 0)
				i++;
			if (i < 2)
				i = 2;
			return i;
		}

		public void reduce(IntWritable key, Iterable<Text> values, Context ctx)
				throws IOException, InterruptedException {
			Iterator<Text> iter = values.iterator();
			while (iter.hasNext()) {
				Text text = iter.next();
				String row = text.toString();
				Pair<Tweet, Histogram> data = MRDataFormat.parseTimeSeries(row);
				long width = data.arg1.groupby(InvertedIndexBuilder.granu)
						.width();
				mop.write("part" + part(width), text, NullWritable.get(),
						"part" + part(width) + "/");
			}
		}
	}

	public static void run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = new Job(conf);
		job.setJobName("按照lifetime对时间序列进行分割");

		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(TimeSeriesMapper.class);
		job.setReducerClass(TimeSeriesReducer.class);

		for (int i = 0; i < Math.log(1000000) / Math.log(2); i++) {
			MultipleOutputs.addNamedOutput(job, String.format("part%d", i),
					TextOutputFormat.class, Text.class, Text.class);
		}
		job.setNumReduceTasks(20);
		TextInputFormat.setInputPaths(job, new Path(args[0]));
		Path output = new Path(args[1]);
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(output)) {
			fs.delete(output, true);
		}
		TextOutputFormat.setOutputPath(job, output);
		job.waitForCompletion(true);
	}

	// inputDir(the histogram,也就是微博的转发序列), outDir1
	public static void main(String[] args) throws Exception {
		run(args);
	}
}
