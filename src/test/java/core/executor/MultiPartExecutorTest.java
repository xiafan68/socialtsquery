package core.executor;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import Util.Profile;
import segmentation.Interval;
import core.index.IndexReader;
import core.index.TempKeywordQuery;

public class MultiPartExecutorTest {
	@Test
	public void test() {
		Path dir = new Path("/Users/xiafan/temp/output/");
		dir = new Path("/home/xiafan/temp/invindex_parts");
		Configuration conf = new Configuration();

		IndexReader indexReader = new IndexReader();
		try {
			indexReader.addPartitions(dir, conf);
			MultiPartitionExecutor executor = new MultiPartitionExecutor(indexReader);
			executor.setMaxLifeTime((int) Math.pow(2, 31));
			String keyword[] = new String[] { "城管", "暴力" };
			Interval window = new Interval(1, 749782, 750493, 1);
			TempKeywordQuery query = new TempKeywordQuery(keyword, window, 10);
			executor.query(query);
			Iterator<Interval> res = executor.getAnswer();
			while (res.hasNext())
				System.out.println(res.next());
			indexReader.close();
			System.out.println(Profile.instance.toJSON());
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
