package core.index;

import java.io.IOException;
import java.util.Iterator;

import org.junit.Test;

import core.index.MemTable.SSTableMeta;
import core.index.octree.IOctreeIterator;
import core.index.octree.OctreeNode;
import Util.Configuration;

public class DiskSSTableReaderTest {
	@Test
	public void test() throws IOException {
		Configuration conf = new Configuration();
		conf.load("conf/index.conf");
		LSMOInvertedIndex index = new LSMOInvertedIndex(conf);
		DiskSSTableReader reader = new DiskSSTableReader(index,
				new SSTableMeta(2, 1));
		reader.init();
		Iterator<Integer> iter = reader.keySetIter();
		while (iter.hasNext()) {
			System.out.println(iter.next());
		}

		IOctreeIterator scanner = reader.getPostingListScanner(0);
		OctreeNode cur = null;
		int size = 0;
		while (scanner.hasNext()) {
			cur = scanner.next();
			size += cur.size();
			System.out.println(size);
			System.out.println(cur);
		}
	}
}
