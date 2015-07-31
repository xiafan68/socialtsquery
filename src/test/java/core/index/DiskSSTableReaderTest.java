package core.index;

import java.io.IOException;
import java.util.Iterator;

import org.junit.Assert;
import org.junit.Test;

import Util.Configuration;
import core.index.MemTable.SSTableMeta;
import core.index.octree.IOctreeIterator;
import core.index.octree.OctreeNode;

public class DiskSSTableReaderTest {
	@Test
	public void test() throws IOException {
		Configuration conf = new Configuration();
		conf.load("conf/index.conf");
		LSMOInvertedIndex index = new LSMOInvertedIndex(conf);
		DiskSSTableReader reader = new DiskSSTableReader(index,
				new SSTableMeta(0, 0));
		reader.init();
		Iterator<Integer> iter = reader.keySetIter();
		while (iter.hasNext()) {
			System.out.println(iter.next());
		}

		int size = 0;

		iter = reader.keySetIter();
		while (iter.hasNext()) {
			int key = iter.next();

			IOctreeIterator scanner = reader.getPostingListScanner(key);
			OctreeNode cur = null;
			while (scanner.hasNext()) {
				cur = scanner.next();
				size += cur.size();
				System.out.println(key + " " + cur);
			}
		}
		Assert.assertEquals(conf.getFlushLimit(), size);
	}
}
