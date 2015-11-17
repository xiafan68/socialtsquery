package core.executor;

import java.io.IOException;
import java.util.Date;
import java.util.Iterator;

import org.apache.hadoop.fs.Path;
import org.apache.log4j.PropertyConfigurator;
import org.junit.Test;

import core.commom.TempKeywordQuery;
import core.lsmo.OctreeBasedLSMTFactory;
import core.lsmt.LSMTInvertedIndex;
import core.lsmt.PartitionMeta;
import segmentation.Interval;
import util.Configuration;
import util.MyFile;

public class PartitionExecutorTest1 {
	public static final int[] widths =
	// new int[] { 2, 8, 12, 24, 48, 48 * 7, 48 * 30 };
	new int[] { 48 * 30 };
	public static final int[] ks = new int[] { 10, 20, 50, 100, 150, 200, 250, 300, 350, 400 };
	public static final int[] offsets = new int[] { 0, 2, 12, 24, 48, 48 * 7, 48 * 30 };

	public static void main(String args[]) {
		String path = "./data/nqueryseed.txt";
		MyFile myFile = new MyFile(path, "utf-8");
		try {
			Path dir = new Path("/Users/xiafan/temp/output/");
			dir = new Path("/home/xiafan/文档/dataset/output");
			dir = new Path("file:///D:\\RWork\\微博查询\\part");
			dir = new Path("hdfs://10.11.1.42:9000/home/xiafan");
			dir = new Path("hdfs://10.11.1.42:9000/home/xiafan/expr/sina/invindex");
			PropertyConfigurator.configure("conf/log4j-server2.properties");
			Configuration conf = new Configuration();
			conf.load("conf/index_twitter.conf");
			LSMTInvertedIndex indexReader = new LSMTInvertedIndex(conf);
			// indexReader.addPartition(new PartitionMeta(31), dir, conf);
			WeightedQueryExecutor executor = new WeightedQueryExecutor(indexReader);
			executor.setMaxLifeTime((int) Math.pow(2, 31));

			String record = myFile.readLine();
			int test_size = 0;
			while (record != null && test_size < 20) {
				System.out.println("*********************");
				test_size++;
				String tmp[] = record.split("\t");
				String keyword1 = tmp[0], keyword2 = tmp[1];
				int offset_start = Integer.parseInt(tmp[2]);
				for (int u = 0; u < offsets.length; u++) {
					int offset = offset_start + offsets[u];// 窗口起始位置
					for (int t = 0; t < widths.length; t++) {
						for (int v = 0; v < ks.length; v++) {

							String keyword[] = new String[] { keyword1, keyword2 };
							Interval window = new Interval(1, offset, offset + widths[t], 1);
							Date start = new Date();
							TempKeywordQuery query = new TempKeywordQuery(keyword, window, ks[v]);
							executor.query(query);
							Iterator<Interval> res = executor.getAnswer();
							Date end = new Date();
							System.out.println(end.getTime() - start.getTime());
							while (res.hasNext())
								System.out.println(res.next());
						}
					}
				}

				record = myFile.readLine();
			}
			indexReader.close();
			myFile.close();

		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public long query(String keyword1, String keyword2, int window_start, int window_width, int topk) {
		return 111;
	}

	@Test
	public void test() throws IOException {
		Path dir = new Path("/Users/xiafan/temp/output/");
		dir = new Path("/home/xiafan/文档/dataset/output");
		dir = new Path("file:///D:\\RWork\\微博查询\\part");
		dir = new Path("hdfs://10.11.1.42:9000/home/xiafan");
		dir = new Path("hdfs://10.11.1.42:9000/home/xiafan/expr/sina/invindex");

		PropertyConfigurator.configure("conf/log4j-server2.properties");
		Configuration conf = new Configuration();
		conf.load("conf/index_twitter.conf");
		LSMTInvertedIndex indexReader = new LSMTInvertedIndex(conf);
		try {
			// indexReader.addPartition(new PartitionMeta(31), dir, conf);
			WeightedQueryExecutor executor = new WeightedQueryExecutor(indexReader);
			executor.setMaxLifeTime((int) Math.pow(2, 31));

			String keyword[] = new String[] { "伦敦", "奥运会" };
			Interval window = new Interval(1, 749782, 750493, 1);
			Date start = new Date();
			TempKeywordQuery query = new TempKeywordQuery(keyword, window, 10);
			executor.query(query);
			Iterator<Interval> res = executor.getAnswer();
			Date end = new Date();
			System.out.println(end.getTime() - start.getTime());

			while (res.hasNext())
				System.out.println(res.next());
			indexReader.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
