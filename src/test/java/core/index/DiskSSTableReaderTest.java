package core.index;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;

import org.junit.Assert;
import org.junit.Test;

import Util.Configuration;
import common.MidSegment;
import core.index.MemTable.SSTableMeta;
import core.index.octree.IOctreeIterator;
import core.index.octree.OctreeNode;

public class DiskSSTableReaderTest {
	/**
	 * 遍历一个索引文件
	 * 
	 * @throws IOException
	 */
	@Test
	public void test() throws IOException {
		Configuration conf = new Configuration();
		conf.load("conf/index.conf");
		LSMOInvertedIndex index = new LSMOInvertedIndex(conf);
		int level = 3;
		int expect = (conf.getFlushLimit() + 1) * (1 << level);
		DiskSSTableReader reader = null;
		HashSet<MidSegment> segs = new HashSet<MidSegment>();
		reader = new DiskSSTableReader(index, new SSTableMeta(7, level));
		reader.init();
		readerTest(reader, segs);
		reader = new DiskSSTableReader(index, new SSTableMeta(15, level));
		reader.init();
		readerTest(reader, segs);
		System.out.println(segs.size());

		level = 4;
		reader = new DiskSSTableReader(index, new SSTableMeta(15, level));
		reader.init();
		HashSet<MidSegment> mergeSegs = new HashSet<MidSegment>();
		readerTest(reader, mergeSegs);
		segs.removeAll(mergeSegs);
		System.out.println("not find " + segs);
		readerTest(reader, conf, level);
	}

	public static void readerTest(ISSTableReader reader,
			HashSet<MidSegment> segs) throws IOException {
		Iterator<Integer> iter = reader.keySetIter();
		while (iter.hasNext()) {
			int key = iter.next();
			IOctreeIterator scanner = reader.getPostingListScanner(key);
			OctreeNode cur = null;
			while (scanner.hasNext()) {
				cur = scanner.next();
				for (MidSegment seg : cur.getSegs()) {
					segs.add(seg);
				}
			}
		}
	}

	public static void readerTest(ISSTableReader reader, Configuration conf,
			int level) throws IOException {
		int expect = (conf.getFlushLimit() + 1) * (1 << level);
		int size = 0;
		Iterator<Integer> iter = reader.keySetIter();
		OctreeNode pre = null;
		while (iter.hasNext()) {
			int key = iter.next();
			pre = null;
			IOctreeIterator scanner = reader.getPostingListScanner(key);
			OctreeNode cur = null;
			while (scanner.hasNext()) {
				cur = scanner.next();
				if (pre != null) {
					if (pre.getEncoding().compareTo(cur.getEncoding()) >= 0)
						System.out.println(key + "\n" + pre + "\n" + cur);
					Assert.assertTrue(pre.getEncoding().compareTo(
							cur.getEncoding()) < 0);
				}
				pre = cur;
				size += cur.size();
				// System.out.println(key + " " + cur);
			}
		}
		System.out.println("total size:" + size);
		Assert.assertEquals(expect, size);
	}
}
