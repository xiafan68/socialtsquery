package core.executor;

import org.junit.Test;

public class MultiPartExecutorTest {
	@Test
	public void test() {
		/*Path dir = new Path("/Users/xiafan/temp/output/");
		dir = new Path("/home/xiafan/temp/invindex_parts");
		Configuration conf = new Configuration();

		IndexReader indexReader = new IndexReader();
		try {
			indexReader.addPartitions(dir, conf);
			MultiPartitionExecutor executor = new MultiPartitionExecutor(indexReader);
			executor.setMaxLifeTime((int) Math.pow(2, 31));
			String keyword[] = new String[] { "伦敦","奥运会"};
			Interval window = new Interval(1, 747056, 748056, 1);
			TempKeywordQuery query = new TempKeywordQuery(keyword, window, 400);
			executor.query(query);
			Iterator<Interval> res = executor.getAnswer();
			while (res.hasNext())
				System.out.println(res.next());
			indexReader.close();
			System.out.println(Profile.instance.toJSON());
		} catch (IOException e) {
			e.printStackTrace();
		}*/
	}
}
