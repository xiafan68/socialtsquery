package core.index;

import java.io.IOException;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import segmentation.Interval;

public class IndexReaderTest {
	@Test
	public void postingListCursorTest() throws IOException {
		String dataDir = "/home/xiafan/文档/dataset/output";
		// dataDir = "/Users/xiafan/temp/output/";
		Path dir = new Path(dataDir);
		Configuration conf = new Configuration();
		/**
		 * 创建
		 */

		IndexReader indexReader = new IndexReader();
		try {
			PartitionMeta meta = new PartitionMeta(16);
			indexReader.addPartition(new PartitionMeta(16), dir, conf);
			PostingListCursor cursor = indexReader.cursor("城管", new Interval(0,
					0, 10000000, 0), meta);

			int i = 0;
			MidSegment pre = null;
			while (cursor.hasNext()) {
				MidSegment cur = cursor.next();
				if (pre != null) {
					int preMax = Math.max(pre.getStartCount(),
							pre.getEndCount());
					int curMax = Math.max(cur.getStartCount(),
							cur.getEndCount());
					System.out.println(cur);
					Assert.assertTrue(preMax >= curMax);
				}
				pre = cur;
				if (i++ > 10)
					break;
			}
			System.out.println(i);
			indexReader.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
