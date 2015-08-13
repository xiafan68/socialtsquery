package core.mr;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Random;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.Logger;

import segmentation.ISegmentation.ISegSubscriber;
import segmentation.Interval;
import segmentation.SWSegmentation;
import segmentation.Segment;
import shingle.StopWordFilter;
import shingle.TextShingle;
import xiafan.util.Histogram;
import xiafan.util.Pair;
import common.MidSegment;
import common.Tweet;
import core.lsmt.IndexWriter;
import extract.TimeSeriesMRJob;
import extract.common.MRDataFormat;

/**
 * 并行构建索引的类，输入为每条微博的时间序列数据
 * 
 * @author xiafan
 * 
 */
public class InvertedIndexBuilder {
	private static Logger logger = Logger.getLogger(TimeSeriesMRJob.class);
	private static String INDEX_DIR_FIELD = "index.dir";
	public static int granu = 1000 * 60 * 30;// 30min

	public static class BuilderMapper extends
			Mapper<LongWritable, Text, Text, MidSegment> {
		TextShingle shingle;
		StopWordFilter filter = new StopWordFilter();

		@Override
		public void setup(Context ctx) {
			try {
				Path[] paths = DistributedCache.getLocalCacheFiles(ctx
						.getConfiguration());
				if (paths != null)
					for (Path path : paths) {
						BufferedReader reader = new BufferedReader(
								new FileReader(path.toString()));
						filter.load(reader);
					}
				shingle = new TextShingle(filter);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		Text outValue = new Text();

		@Override
		public void map(LongWritable key, Text value, final Context ctx)
				throws IOException {

			try {
				final Pair<Tweet, Histogram> data = MRDataFormat
						.parseTimeSeries(value.toString());
				if (data == null)
					return;

				final List<String> keywords = shingle.shingling(data.arg0
						.getContent());

				final long midTmp = Long.parseLong(data.arg0.getMid());
				SWSegmentation seg = new SWSegmentation(midTmp, 5, null,
						new ISegSubscriber() {
							public void newSeg(Interval preInv, Segment seg) {
								for (String keyword : keywords) {
									try {
										ctx.write(new Text(keyword),
												new MidSegment(midTmp, seg));
									} catch (IOException e) {
									} catch (InterruptedException e) {
										// TODO Auto-generated catch block
										e.printStackTrace();
									}
								}
							}
						});
				Iterator<Entry<Double, Integer>> iter = data.arg1
						.groupby(granu).iterator();
				while (iter.hasNext()) {
					Entry<Double, Integer> entry = iter.next();
					seg.advance(entry.getKey().intValue(), entry.getValue());
				}
				seg.finish();
			} catch (Exception ex) {

			}

		}
	}

	public static class BuilderReducer extends
			Reducer<Text, MidSegment, Text, Text> {
		Text outKey = new Text();
		Text outValue = new Text();

		IndexWriter writer = new IndexWriter();

		@Override
		public void setup(Context ctx) {
			Random rand = new Random();
			int id = rand.nextInt();
			logger.info("index output dir:"
					+ ctx.getConfiguration().get(INDEX_DIR_FIELD));
			Path dir = new Path(ctx.getConfiguration().get(INDEX_DIR_FIELD));
			try {
				writer.open(ctx.getConfiguration(), dir, Integer.toString(id));
			} catch (IOException e) {
			}
		}

		@Override
		public void cleanup(Context ctx) throws IOException {
			writer.close();
		}

		@Override
		public void reduce(Text key, Iterable<MidSegment> values, Context ctx)
				throws IOException, InterruptedException {
			// 对MidSegment进行排序，hadoop平台只会对key进行排序
			TreeSet<MidSegment> segs = new TreeSet<MidSegment>(
					new Comparator<MidSegment>() {
						public int compare(MidSegment o1, MidSegment o2) {
							return o1.compareTo(o2);
						}
					});
			for (MidSegment value : values) {
				segs.add(new MidSegment(value.mid, value));
			}

			writer.writeToIndex(key.toString(), segs.iterator());
			// for (MidSegment value : segs)
			// ctx.write(key, new Text(value.toString()));
		}
	}

	public static void main(String[] args) throws IOException,
			ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();

		// 必须放在前面
		// conf.set(INDEX_DIR_FIELD, "/home/xiafan/temp/index");
		conf.set(INDEX_DIR_FIELD, args[1]);
		Job job = new Job(conf);
		job.setJarByClass(InvertedIndexBuilder.class);
		job.setJobName("inverted index builder");
		// /Users/xiafan/Downloads/-home-xiafan-expr-sina-sina_series-part-00000

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(MidSegment.class);

		job.setMapperClass(BuilderMapper.class);
		job.setReducerClass(BuilderReducer.class);

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
}
