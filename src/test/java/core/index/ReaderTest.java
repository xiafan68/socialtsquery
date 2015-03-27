package core.index;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Iterator;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import segmentation.Interval;
import core.index.IndexWriter.DirEntry;

public class ReaderTest {
	@Test
	public void loadDir() throws IOException {
		DataInputStream dis = new DataInputStream(
				new BufferedInputStream(new FileInputStream(
						"/Users/xiafan/temp/output/-2072498975.dir")));

		int i = 0;
		while (dis.available() > 0) {
			i++;
			DirEntry entry = new DirEntry();
			entry.read(dis);
			System.out.println(entry);
		}
		System.out.println(i);
		dis.close();
	}

	@Test
	public void postingListCursorTest() throws IOException {
		Path dir = new Path("/Users/xiafan/temp/output/");
		String part = "389375622";
		Configuration conf = new Configuration();
		IndexFileGroupReader indexReader = new IndexFileGroupReader(
				new PartitionMeta(0));
		try {
			indexReader.open(dir, part, conf);
			DataInputStream dis = new DataInputStream(
					new BufferedInputStream(new FileInputStream(new Path(dir,
							part + ".dir").toString())));

			while (dis.available() > 0) {
				DirEntry entry = new DirEntry();
				entry.read(dis);
				Interval window = new Interval(1, 0, 100000000, 1);
				Iterator<MidSegment> res = indexReader.cursor(entry.keyword,
						window);
				int count = 0;
				System.out.println(entry);
				MidSegment pre = null;
				while (res.hasNext()) {
					MidSegment cur = res.next();
					if (pre != null) {
						int preMax = Math.max(pre.getStartCount(),
								pre.getEndCount());
						int curMax = Math.max(cur.getStartCount(),
								cur.getEndCount());
						// System.out.println(pre.compareTo(cur));
						Assert.assertTrue(preMax >= curMax);
					}
					pre = cur;
					System.out.println(count + " --- " + cur);
					count++;
				}
				Assert.assertEquals(entry.size, count);
				System.out.println("=============" + count);
			}
			indexReader.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
