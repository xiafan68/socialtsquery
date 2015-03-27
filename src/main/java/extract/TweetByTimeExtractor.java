package extract;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import weibo4j.WeiboException;
import weibo4j.model.Status;
import weibo4j.model.StatusWapper;
import xiafan.time.TimeRange;
import common.NormFormat;
import extract.common.Tool;

/**
 * 用以从原始json格式的数据中抽取所有转发某段时间内创建的微博
 * 
 * @author xiafan
 * 
 */
public class TweetByTimeExtractor {
	public static List<TimeRange> ranges = new LinkedList<TimeRange>();
	static {
		ranges.add(new TimeRange(new Date(2012 - 1900, 1, 1, 0, 0, 0),
				new Date(2012 - 1900, 12, 30, 23, 59, 59)));
	}

	public static class Map extends
			Mapper<LongWritable, Text, LongWritable, Text> {
		List<Status> statusList = new ArrayList<Status>();
		Status rtStatus = null;
		Status text = null;

		LongWritable x = new LongWritable();
		Text y = new Text();

		public void map(LongWritable key, Text value, Context ctx)
				throws IOException {
			try {
				// 解析status列表
				statusList.clear();
				if (value.toString().startsWith("{")) {
					StatusWapper wapper = Status.constructWapperStatus(Tool
							.removeEol(value.toString()));
					if (wapper != null) {
						statusList = wapper.getStatuses();
					}

				} else if (value.toString().startsWith("[")) {
					statusList = Status.constructStatuses(Tool.removeEol(value
							.toString()));
				}

				if (statusList == null) {
					return;
				}

				for (Status status : statusList) {
					boolean include = false;
					for (TimeRange range : ranges) {
						Status rt = status.getRetweetedStatus();
						if (rt != null && status.getCreatedAt() != null
								&& range.contains(status.getCreatedAt())) {
							// if (rt.getCreatedAt() != null
							// && range.contains(rt.getCreatedAt())) {
							// include = true;
							// break;
							// }
							include = true;
							break;
						}
					}

					if (include) {
						String ret = NormFormat.transToNorm(status);
						if (!ret.isEmpty()) {
							x.set(status.getCreatedAt().getTime());
							y.set(ret);
							ctx.write(x, y);
						}
					}
				}
			} catch (WeiboException e) {
				e.printStackTrace();
			} catch (weibo4j.model.WeiboException e) {
				e.printStackTrace();
			} catch (Exception ex) {

			}
		}
	}

	public static class Reduce extends
			Reducer<LongWritable, Text, NullWritable, Text> {
		public void reduce(LongWritable key, Iterable<Text> values, Context ctx)
				throws IOException, InterruptedException {
			Iterator<Text> iter = values.iterator();
			while (iter.hasNext()) {
				ctx.write(NullWritable.get(), iter.next());
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = new Job(conf);
		job.setJobName("TweetByTimeExtractor");

		FileSystem fs = FileSystem.get(conf);
		List<FileStatus> files = new ArrayList<FileStatus>();
		for (FileStatus status : fs.listStatus(new Path(args[0]))) {
			files.add(status);
		}

		for (int i = 0; i < files.size(); i += 10) {
			conf = new Configuration();
			job = new Job(conf);
			job.setJobName("TweetByTimeExtractor");
			for (FileStatus status : files.subList(i,
					Math.min(i + 10, files.size()))) {
				FileInputFormat.addInputPath(job, status.getPath());
			}

			job.setOutputKeyClass(LongWritable.class);
			job.setOutputValueClass(Text.class);
			job.setMapperClass(Map.class);
			job.setReducerClass(Reduce.class);

			conf.setInt("mapred.min.split.size", 268435456);
			conf.setInt("mapred.task.timeout", 2400000);

			job.setNumReduceTasks(4);
			// FileOutputFormat.setOutputPath(conf, new Path(args[1]));
			Path output = new Path(args[1], Integer.toString(i));
			fs = FileSystem.get(conf);
			if (fs.exists(output)) {
				fs.delete(output, true);
			}
			FileOutputFormat.setOutputPath(job, output);
			job.waitForCompletion(true);
		}
	}
}
