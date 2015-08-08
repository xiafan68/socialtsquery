package core.index.octree;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;

import org.apache.log4j.PropertyConfigurator;
import org.junit.Assert;
import org.junit.Test;

import Util.Configuration;
import common.MidSegment;
import core.commom.Encoding;
import core.commom.Point;
import core.index.DiskSSTableReader;
import core.index.LSMOInvertedIndex;
import core.index.MemTable.SSTableMeta;
import segmentation.Interval;

public class OctreePostingListIterTest {
	@Test
	public void test() throws IOException {
		PropertyConfigurator.configure("conf/log4j-server.properties");

		Configuration conf = new Configuration();
		conf.load("conf/index.conf");
		LSMOInvertedIndex index = new LSMOInvertedIndex(conf);
		DiskSSTableReader reader = new DiskSSTableReader(index,
				new SSTableMeta(255, 8));
		reader.init();
		Iterator<Integer> iter = reader.keySetIter();
		while (iter.hasNext()) {
			System.out.println(iter.next());
		}
		int ts = 696602;
		int te = 696622;
		Interval window = new Interval(0, ts, te, 0);

		int expectNodeNum = 0;
		iter = reader.keySetIter();
		HashSet<Encoding> expectCodes = new HashSet<Encoding>();
		Encoding code = null;
		Encoding exCode = new Encoding(new Point(696592, 696592, 8), 3);

		while (iter.hasNext()) {
			int expect = 0;
			int key = iter.next();
			System.out.println(key);
			OctreeNode cur = null;
			// FileOutputStream fos = new FileOutputStream("/tmp/visit.log");
			// System.setOut(new PrintStream(fos));
			// 遍历所有的node，找到相关的segs
			IOctreeIterator scanner = reader.getPostingListScanner(key);
			while (scanner.hasNext()) {
				cur = scanner.next();
				code = cur.getEncoding();
				if (code.getX() < te && code.getY() + code.getEdgeLen() > ts) {
					expectNodeNum++;
					// System.out.print("hit ");
					System.out.println(code);
					System.out
							.println(((DiskOctreeIterator) scanner).nextBucketID
									+ " "
									+ ((DiskOctreeIterator) scanner).curIdx);
				} else {
					// System.out.print("not hit ");
				}

				// System.out.println(code);

				boolean print = false;

				for (MidSegment seg : cur.getSegs()) {
					if (seg.getStart() <= window.getEnd()
							&& seg.getEndTime() >= window.getStart()) {
						expect++;
						if (!print) {
							expectCodes.add(code);
							if (expectCodes.contains(exCode)) {
								System.out.println();
							}
							print = true;
						}
					}
				}
				if (expectCodes.contains(exCode)) {
					System.out.println();
				}
			}

			// fos.close();

			// fos = new FileOutputStream("/tmp/skip.log");
			// System.setOut(new PrintStream(fos));
			System.out.println(" skip");
			int size = 0;
			scanner = reader.getPostingListIter(key, window.getStart(),
					window.getEnd());
			while (scanner.hasNext()) {
				boolean print = false;
				cur = scanner.next();
				code = cur.getEncoding();
				if (code.getX() < te && code.getY() + code.getEdgeLen() > ts) {
					expectNodeNum++;
					// System.out.println(((OctreePostingListIter)
					// scanner).nextID);
					System.out.println(code);
				}
				for (MidSegment seg : cur.getSegs()) {
					if (seg.getStart() <= window.getEnd()
							&& seg.getEndTime() >= window.getStart()) {
						size++;
						if (!print) {
							if (!expectCodes.remove(code)) {
								System.out.println("new visit++++" + code);
							}
							print = true;
						}
					}
				}
			}
			for (Encoding encoding : expectCodes) {
				System.out.println("not visisted " + encoding);
			}
			// fos.close();
			System.out.println("expected size:" + expect);
			Assert.assertEquals(expect, size);
		}
	}
}
