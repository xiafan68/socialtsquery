package core.executor;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import Util.Profile;
import core.commom.TempKeywordQuery;
import core.lsmt.PartitionMeta;
import segmentation.Interval;

public class PartitionExecutorTest {
	@Test
	public void test() {
		Path dir = new Path("/Users/xiafan/temp/output/");
		dir = new Path("/home/xiafan/文档/dataset/output");
		Configuration conf = new Configuration();

		IndexReader indexReader = new IndexReader();
		try {
			indexReader.addPartition(new PartitionMeta(16), dir, conf);
			WeightedQueryExecutor executor = new WeightedQueryExecutor(indexReader);
			executor.setMaxLifeTime((int) Math.pow(2, 16));
			String keyword[] = new String[] { "伦敦","奥运会"};
			Interval window = new Interval(1, 747056, 748056, 1);
			TempKeywordQuery query = new TempKeywordQuery(keyword, window, 400);
			Profile.instance.start("query");
			executor.query(query);
			Iterator<Interval> res = executor.getAnswer();
			Profile.instance.end("query");
			while (res.hasNext())
				System.out.println(res.next());
			indexReader.close();
			System.out.println(Profile.instance.toJSON());
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
