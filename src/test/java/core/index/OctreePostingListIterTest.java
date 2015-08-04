package core.index;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Iterator;

import org.junit.Assert;
import org.junit.Test;

import Util.Configuration;
import common.MidSegment;
import core.index.MemTable.SSTableMeta;
import core.index.octree.IOctreeIterator;
import core.index.octree.OctreeNode;
import segmentation.Interval;

public class OctreePostingListIterTest {
	@Test
	public void test() throws IOException {
		System.setOut(new PrintStream(new FileOutputStream("/home/xiafan/temp/log/visit.log")));
		Configuration conf = new Configuration();
		conf.load("conf/index.conf");
		LSMOInvertedIndex index = new LSMOInvertedIndex(conf);
		DiskSSTableReader reader = new DiskSSTableReader(index, new SSTableMeta(34, 0));
		reader.init();
		Iterator<Integer> iter = reader.keySetIter();
		while (iter.hasNext()) {
			System.out.println(iter.next());
		}
		Interval window = new Interval(0, 696602, 696622, 0);
		int size = 0;
		int expect = 0;

		iter = reader.keySetIter();
		while (iter.hasNext()) {
			int key = iter.next();
			OctreeNode cur = null;
			// 遍历所有的node，找到相关的segs
			IOctreeIterator scanner = reader.getPostingListScanner(key);
			while (scanner.hasNext()) {
				cur = scanner.next();
				boolean print = false;
				for (MidSegment seg : cur.getSegs()) {
					if (seg.getStart() <= window.getEnd() && seg.getEndTime() >= window.getStart()) {
						expect++;
						if (!print) {
							System.out.println("should visit++++" + cur.getEncoding());
							print = true;
						}
					}
				}
			}

			scanner = reader.getPostingListIter(key, window.getStart(), window.getEnd());
			while (scanner.hasNext()) {
				cur = scanner.next();
				for (MidSegment seg : cur.getSegs()) {
					if (seg.getStart() <= window.getEnd() && seg.getEndTime() >= window.getStart()) {
						size++;
					}
				}
				System.out.println("hit " + key + "  " + cur.getEncoding());
			}
			Assert.assertEquals(expect, size);
		}
	}

}
