package extract;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.Logger;

import rttree.RTTreeConstructor;
import rttree.RTTreeTimeSeries;
import xiafan.datastructure.tree.trie.TrieTree;
import xiafan.util.Histogram;
import xiafan.util.Pair;
import common.NormFormat;
import common.Tweet;

/**
 * 计算每条微博的转发序列
 * 
 * @author xiafan
 * 
 */
public class TimeSeriesMRJob {
	private static Logger logger = Logger.getLogger(TimeSeriesMRJob.class);

	public static class TimeSeriesMapper extends
			Mapper<LongWritable, Text, Text, Text> {
		Text outKey = new Text();
		Text outValue = new Text();

		@Override
		public void map(LongWritable key, Text value, Context ctx)
				throws IOException, InterruptedException {
			xFormat(key, value, ctx);
		}

		/**
		 * 解析夏帆抽取数据的格式
		 * 
		 * @param key
		 * @param value
		 * @param ctx
		 * @throws IOException
		 * @throws InterruptedException
		 */
		public void xFormat(LongWritable key, Text value, Context ctx)
				throws IOException, InterruptedException {
			String row = value.toString();
			int idx = row.indexOf(" ");
			if (idx > 0) {
				String oMid = row.substring(0, idx);
				outKey.set(oMid);
				// output.collect(outKey, value);
				ctx.write(outKey, value);
			}
		}

		/**
		 * 处理马海欣的EventData
		 * 
		 * @param key
		 * @param value
		 * @param output
		 * @param reporter
		 * @throws IOException
		 * @throws InterruptedException
		 */
		public void mFormat(LongWritable key, Text value, Context ctx)
				throws IOException, InterruptedException {
			String row = value.toString();
			Tweet t = NormFormat.transHxmToTweets(row);
			if (t != null) {
				if (t.getoMid().equals("-1") || t.getoMid().isEmpty())
					outKey.set(t.getMid());
				else
					outKey.set(t.getoMid());
				outValue.set(t.toString());
				ctx.write(outKey, outValue);
			}
		}
	}

	public static class TimeSeriesReducer extends
			Reducer<Text, Text, Text, Text> {
		int failCount = 0;
		Text outKey = new Text();
		Text outValue = new Text();

		@Override
		public void reduce(Text key, Iterable<Text> values, Context ctx)
				throws IOException, InterruptedException {
			xReduce(key, values, ctx);
		}

		public void xReduce(Text key, Iterable<Text> values, Context ctx)
				throws IOException, InterruptedException {
			Iterator<Text> iter = values.iterator();
			Tweet ot = null;
			List<Tweet> tweets = new ArrayList<Tweet>();
			while (iter.hasNext()) {
				String line = iter.next().toString();
				Pair<Long, Tweet> rt = null;
				try {
					rt = NormFormat.transToTweetOfSimple(line);
					if (rt.arg1.getoMid().equals(rt.arg1.getMid()))
						ot = rt.arg1;
					else
						tweets.add(rt.arg1);
				} catch (Exception e) {

				}
			}

			if (ot == null || tweets.size() == 0)
				return;
			TrieTree<Tweet> tree = RTTreeConstructor.parse(ot, tweets);
			RTTreeTimeSeries series = new RTTreeTimeSeries(tree);
			while (series.hasNext()) {
				Pair<Tweet, Histogram> pair = series.next();
				// System.out.println(pair.arg0 + " " + pair.arg1.toString());
				outKey.set(pair.arg0.toString());
				outValue.set(pair.arg1.toString());
				// output.collect(outKey, outValue);
				ctx.write(outKey, outValue);
			}
		}

		public void mReduce(Text key, Iterable<Text> values, Context ctx)
				throws IOException, InterruptedException {
			Iterator<Text> iter = values.iterator();
			Tweet ot = null;
			List<Tweet> tweets = new ArrayList<Tweet>();
			while (iter.hasNext()) {
				Tweet t = new Tweet();
				t.parse(iter.next().toString());
				if (t.getoMid().equals("-1") || t.getoMid().isEmpty()) {
					ot = t;
				} else {
					tweets.add(t);
				}
			}
			if (ot == null || tweets.size() == 0)
				return;
			TrieTree<Tweet> tree = RTTreeConstructor.parse(ot, tweets);
			RTTreeTimeSeries series = new RTTreeTimeSeries(tree);
			while (series.hasNext()) {
				Pair<Tweet, Histogram> pair = series.next();
				// System.out.println(pair.arg0 + " " + pair.arg1.toString());
				outKey.set(pair.arg0.toString());
				outValue.set(pair.arg1.toString());
				// output.collect(outKey, outValue);
				ctx.write(outKey, outValue);
			}
		}
	}

	static Pattern regex = Pattern.compile("part-[0-9]*");

	private static void addInput(final FileSystem fs, Job job, Path input)
			throws IOException {
		FileStatus[] statuses = fs.listStatus(input, new PathFilter() {
			public boolean accept(Path arg0) {
				try {
					if (fs.isFile(arg0)) {
						if (regex.matcher(arg0.getName()).matches())
							return true;
						return false;
					}
				} catch (IOException e) {
				}
				return true;
			}
		});
		for (FileStatus status : statuses) {
			if (status.isDir()) {
				addInput(fs, job, status.getPath());
			} else {
				TextInputFormat.addInputPath(job, status.getPath());
				logger.info("add input path: " + status.getPath().toString());
			}
		}
	}

	public static void run(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = new Job(conf);
		job.setJobName("compute time series for each tweet");
		job.setJarByClass(TimeSeriesMRJob.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(TimeSeriesMapper.class);
		job.setReducerClass(TimeSeriesReducer.class);

		job.setNumReduceTasks(20);

		FileSystem fs = FileSystem.get(conf);
		Path input = new Path(args[0]);
		addInput(fs, job, input);
		Path output = new Path(args[1]);

		if (fs.exists(output)) {
			fs.delete(output, true);
		}
		TextOutputFormat.setOutputPath(job, output);
		job.waitForCompletion(true);
	}

	// inputDir(the histogram), outDir1
	public static void main(String[] args) throws Exception {
		// System.out.println(regex.matcher("part-000").matches());
		run(args);
	}
}
