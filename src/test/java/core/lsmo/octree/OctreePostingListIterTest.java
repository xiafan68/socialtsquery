package core.lsmo.octree;

import java.io.IOException;
import java.util.Iterator;

import org.apache.log4j.PropertyConfigurator;
import org.junit.Assert;
import org.junit.Test;

import Util.Configuration;
import common.MidSegment;
import core.commom.Encoding;
import core.lsmo.DiskSSTableBDBReader;
import core.lsmo.OctreeBasedLSMTFactory;
import core.lsmt.IMemTable.SSTableMeta;
import core.lsmt.LSMTInvertedIndex;
import core.lsmt.WritableComparableKey;
import segmentation.Interval;

public class OctreePostingListIterTest {

	public int[] ExpectedResult(WritableComparableKey key, DiskSSTableBDBReader reader, Interval window)
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
		return ret;
	}

	public int[] queryResult(WritableComparableKey key, DiskSSTableBDBReader reader, Interval window)
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
	public void test() throws IOException {
		PropertyConfigurator.configure("conf/log4j-server.properties");

		Configuration conf = new Configuration();
		conf.load("conf/index_twitter.conf");

		LSMTInvertedIndex index = new LSMTInvertedIndex(conf, OctreeBasedLSMTFactory.INSTANCE);
		index.init();

		DiskSSTableBDBReader reader = (DiskSSTableBDBReader) index.getSSTableReader(index.getVersion(),
				new SSTableMeta(127, 7));

		reader.init();
		Iterator<WritableComparableKey> iter = reader.keySetIter();
		while (iter.hasNext()) {
			System.out.println(iter.next());
		}
		int ts = 676602;
		int te = 696622;
		Interval window = new Interval(0, ts, te, 0);

		System.out.println("begin to verify");
		System.out.flush();
		iter = reader.keySetIter();
		// 遍历所有的posting list
		try {
			while (iter.hasNext()) {
				WritableComparableKey key = iter.next();
				int[] expect = ExpectedResult(key, reader, window);
				int[] answer = queryResult(key, reader, window);
				// fos.close();
				System.out.println("expected size:" + expect[0] + "," + expect[1]);
				Assert.assertEquals(expect[0], answer[0]);
				Assert.assertEquals(expect[1], answer[1]);
			}
		} finally {
			reader.close();
		}
	}
}
