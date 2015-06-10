package perf;

import java.io.IOException;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import segmentation.Interval;
import xiafan.util.Pair;
import Util.MyFile;
import Util.Profile;
import core.commom.TempKeywordQuery;
import core.executor.PartitionExecutor;
import core.index.IndexReader;
import core.index.PartitionMeta;
import expr.QueryGen;

public class PerformanceTest {
	public static final int[] widths = new int[] { 2, 8, 12, 24, 48, 48 * 7,
			48 * 30 };
	// new int[] { 48 * 30 };
	public static final int[] ks = new int[] { 10, 20, 50, 100, 150, 200, 250,
			300, 350, 400 };
	public static final int[] offsets = new int[] { 0, 2, 12, 24, 48, 48 * 7,
			48 * 30 };

	public static void main(String args[]) {
		String path = "./data/nqueryseed.txt";
		path = "/home/xiafan/KuaiPan/dataset/time_series/nqueryseed.txt";
		MyFile myFile = new MyFile(path, "utf-8");
		try {
			Path dir = new Path("/Users/xiafan/temp/output/");
			dir = new Path("/home/xiafan/文档/dataset/output");
			dir = new Path("file:///D:\\RWork\\微博查询\\part");
			dir = new Path("file:///home/xiafan/temp/invindex");
			// dir = new
			// Path("hdfs://10.11.1.42:9000/home/xiafan/expr/sina/invindex");
			Configuration conf = new Configuration();
			IndexReader indexReader = new IndexReader();
			indexReader.addPartition(new PartitionMeta(31), dir, conf);

			QueryGen gen = new QueryGen(10);
			gen.loadQueryWithTime(path);

			for (int u = 0; u < offsets.length; u++) {
				for (int t = 0; t < widths.length; t++) {
					// System.out.println("**** width: " + widths[t]);
					for (int v = 0; v < ks.length; v++) {
						gen.resetCur();
						while (gen.hasNext()) {
							Pair<List<String>, Integer> cur = gen.nextQuery();
							int offset = cur.arg1 + offsets[u];
							PartitionExecutor executor = new PartitionExecutor(
									indexReader);
							executor.setMaxLifeTime((int) Math.pow(2, 31));
							Interval window = new Interval(1, offset, offset
									+ widths[t], 1);
							long start = System.currentTimeMillis();

							TempKeywordQuery query = new TempKeywordQuery(
									cur.arg0.toArray(new String[cur.arg0.size()]),
									window, ks[v]);
							Profile.instance.start("query");
							executor.query(query);
							Iterator<Interval> res = executor.getAnswer();
							Profile.instance.end("query");
							long end = System.currentTimeMillis();

							int count = 0;
							while (res.hasNext()) {
								// System.out.println(res.next());
								res.next();
								count++;
							}
							long time = end - start;
							// System.out.println(String.format("%d,%d,%d",
							// time, count, ks[v]));
							if (time > 1500)
								System.err.println(query.toString() + ";"
										+ time);
							else {
								System.out.println(query.toString() + ";"
										+ time);
							}
							System.out.println(Profile.instance.toJSON());
							Profile.instance.reset();
							System.gc();
						}

					}
				}

			}
			indexReader.close();
			myFile.close();

		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public long query(String keyword1, String keyword2, int window_start,
			int window_width, int topk) {
		return 111;
	}

	@Test
	public void test() {
		Path dir = new Path("/Users/xiafan/temp/output/");
		dir = new Path("/home/xiafan/文档/dataset/output");
		dir = new Path("file:///D:\\RWork\\微博查询\\part");
		dir = new Path("hdfs://10.11.1.42:9000/home/xiafan");
		dir = new Path("hdfs://10.11.1.42:9000/home/xiafan/expr/sina/invindex");
		Configuration conf = new Configuration();

		IndexReader indexReader = new IndexReader();
		try {
			indexReader.addPartition(new PartitionMeta(31), dir, conf);
			PartitionExecutor executor = new PartitionExecutor(indexReader);
			executor.setMaxLifeTime((int) Math.pow(2, 31));

			String keyword[] = new String[] { "日本", "钓鱼岛" };
			Interval window = new Interval(1, 748544, 748544 + 1440, 1);
			Date start = new Date();
			TempKeywordQuery query = new TempKeywordQuery(keyword, window, 10);
			executor.query(query);
			Iterator<Interval> res = executor.getAnswer();
			Date end = new Date();
			System.out.println(end.getTime() - start.getTime());
			while (res.hasNext())
				System.out.println(res.next());

			executor = new PartitionExecutor(indexReader);
			executor.setMaxLifeTime((int) Math.pow(2, 31));
			keyword = new String[] { "中国", "钓鱼岛" };
			window = new Interval(1, 747236, 747236 + 1440, 1);
			start = new Date();
			query = new TempKeywordQuery(keyword, window, 10);
			executor.query(query);
			res = executor.getAnswer();
			end = new Date();
			System.out.println(end.getTime() - start.getTime());
			while (res.hasNext())
				System.out.println(res.next());

			indexReader.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
