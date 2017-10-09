package core.lsmo.octree;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;

import org.apache.log4j.PropertyConfigurator;
import org.junit.Assert;
import org.junit.Test;

import common.MidSegment;
import core.commom.BDBBtree;
import core.commom.Encoding;
import core.commom.WritableComparable;
import core.commom.WritableComparable.StringKey;
import core.lsmo.bdbformat.DiskSSTableBDBReader;
import core.lsmo.internformat.InternOctreeSSTableReader;
import core.lsmt.IMemTable.SSTableMeta;
import core.lsmt.LSMTInvertedIndex;
import segmentation.Interval;
import util.Configuration;

public class OctreePostingListIterTest {

	public int[] ExpectedResult(WritableComparable key, DiskSSTableBDBReader reader, Interval window)
			throws IOException {
		int[] ret = new int[] { 0, 0 };
		System.out.println(key);
		Encoding code = null;
		OctreeNode cur = null;
		// FileOutputStream fos = new FileOutputStream("/tmp/visit.log");
		// System.setOut(new PrintStream(fos));
		// 遍历所有的node，找到相关的segs
		IOctreeIterator scanner = reader.getPostingListScanner(key);
		while (scanner.hasNext()) {
			cur = scanner.nextNode();
			code = cur.getEncoding();
			if (code.getX() <= window.getEnd() && code.getY() + code.getEdgeLen() >= window.getStart()) {
				ret[0]++;
				// System.out.print("hit ");
				// System.out.println(code);
				// System.out.println(((DiskOctreeIterator)
				// scanner).nextBucketID);
			} else {
				// System.out.print("not hit ");
			}

			for (MidSegment seg : cur.getSegs()) {
				if (seg.getStart() <= window.getEnd() && seg.getEndTime() >= window.getStart()) {
					ret[1]++;
				}
			}
		}
		scanner.close();
		return ret;
	}

	public int[] queryResult(WritableComparable key, DiskSSTableBDBReader reader, Interval window)
			throws IOException {
		int[] ret = new int[] { 0, 0 };
		IOctreeIterator scanner = reader.getPostingListIter(key, window.getStart(), window.getEnd());
		Encoding code = null;
		OctreeNode cur = null;
		while (scanner.hasNext()) {
			cur = scanner.nextNode();
			code = cur.getEncoding();
			if (code.getX() <= window.getEnd() && code.getY() + code.getEdgeLen() >= window.getStart()) {
				ret[0]++;
			}
			for (MidSegment seg : cur.getSegs()) {
				if (seg.getStart() <= window.getEnd() && seg.getEndTime() >= window.getStart()) {
					ret[1]++;
				}
			}
		}
		return ret;
	}

	@Test
	public void testKeyword() throws IOException {
		PropertyConfigurator.configure("conf/log4j-server2.properties");

		Configuration conf = new Configuration();
		conf.load("conf/index_twitter.conf");

		LSMTInvertedIndex index = new LSMTInvertedIndex(conf);
		index.init();

		DiskSSTableBDBReader reader = (DiskSSTableBDBReader) index.getSSTableReader(index.getVersion(),
				new SSTableMeta(0, 0));

		reader.init();
		int ts = 676602;
		int te = 696622;
		Interval window = new Interval(0, ts, te, 0);
		System.out.println("begin to verify");
		System.out.flush();
		// 遍历所有的posting list
		try {
			WritableComparable key = new StringKey("#beatcancer");
			int[] expect = ExpectedResult(key, reader, window);
			int[] answer = queryResult(key, reader, window);
			// fos.close();
			System.out.println("expected size:" + expect[0] + "," + expect[1]);
			Assert.assertEquals(expect[0], answer[0]);
			Assert.assertEquals(expect[1], answer[1]);

		} finally {
			index.close();
		}
	}

	@Test
	public void test() throws IOException {
		PropertyConfigurator.configure("conf/log4j-server2.properties");

		Configuration conf = new Configuration();
		conf.load("conf/index_twitter.conf");

		LSMTInvertedIndex index = new LSMTInvertedIndex(conf);
		index.init();

		int ts = 676602;
		int te = 696622;
		Interval window = new Interval(0, ts, te, 0);
		System.out.println("begin to verify");
		System.out.flush();
		// 遍历所有的posting list
		try {
			for (SSTableMeta meta : index.getVersion().diskTreeMetas) {
				DiskSSTableBDBReader reader = (DiskSSTableBDBReader) index.getSSTableReader(index.getVersion(), meta);
				Iterator<WritableComparable> iter = reader.keySetIter();
				while (iter.hasNext()) {
					WritableComparable key = iter.next();
					int[] expect = ExpectedResult(key, reader, window);
					int[] answer = queryResult(key, reader, window);
					// fos.close();
					System.out.println("expected size:" + expect[0] + "," + expect[1]);
					Assert.assertEquals(expect[0], answer[0]);
					Assert.assertEquals(expect[1], answer[1]);
				}
				((BDBBtree.BDBKeyIterator) iter).close();
			}
		} finally {
			index.close();
		}
	}

	@Test
	public void scannerTest() throws IOException {
		PropertyConfigurator.configure("conf/log4j-server2.properties");

		Configuration conf = new Configuration();
		conf.load("conf/index_twitter_intern.conf");

		LSMTInvertedIndex index = new LSMTInvertedIndex(conf);
		index.init();

		System.out.println("begin to verify");
		System.out.flush();
		// 遍历所有的posting list

		try {
			for (SSTableMeta meta : index.getVersion().diskTreeMetas) {
				InternOctreeSSTableReader reader = (InternOctreeSSTableReader) index.getSSTableReader(index.getVersion(),
						meta);
				Iterator<WritableComparable> iter = reader.keySetIter();
				HashSet<Encoding> mids = new HashSet<Encoding>();
				while (iter.hasNext()) {
					WritableComparable key = iter.next();
					IOctreeIterator scanner = (IOctreeIterator) reader.getPostingListScanner(key);
					while (scanner.hasNext()) {
						OctreeNode cur = scanner.nextNode();
						Assert.assertFalse(mids.contains(cur));
						mids.add(cur.getEncoding());
						// System.out.println(scanner.nextNode());
					}
					System.out.println(key + " success");
				}
				((BDBBtree.BDBKeyIterator) iter).close();
			}
		} finally {
			index.close();
		}
	}
}
