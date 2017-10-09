package core.lsmi;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.apache.log4j.PropertyConfigurator;
import org.junit.Assert;
import org.junit.Test;

import common.MidSegment;
import core.commom.BDBBtree;
import core.commom.WritableComparable;
import core.commom.WritableComparable.StringKey;
import core.lsmt.IMemTable.SSTableMeta;
import core.lsmt.LSMTInvertedIndex;
import core.lsmt.postinglist.IPostingListIterator;
import core.lsmt.postinglist.ISSTableReader;
import segmentation.Interval;
import util.Configuration;
import util.Pair;

public class SortedListIterTest {
	public int ExpectedResult(WritableComparable key, ISSTableReader reader, Interval window) throws IOException {
		int ret = 0;
		System.out.println(key);
		Pair<Integer, List<MidSegment>> cur = null;
		// 遍历所有的node，找到相关的segs
		IPostingListIterator scanner = reader.getPostingListScanner(key);
		while (scanner.hasNext()) {
			cur = scanner.next();
			for (MidSegment seg : cur.getValue()) {
				if (seg.getStart() <= window.getEnd() && seg.getEndTime() >= window.getStart()) {
					ret++;
				}
			}
		}
		scanner.close();
		return ret;
	}

	public int queryResult(WritableComparable key, ISSTableReader reader, Interval window) throws IOException {
		int ret = 0;
		IPostingListIterator scanner = reader.getPostingListIter(key, window.getStart(), window.getEnd());

		Pair<Integer, List<MidSegment>> cur = null;
		while (scanner.hasNext()) {
			cur = scanner.next();
			for (MidSegment seg : cur.getValue()) {
				if (seg.getStart() <= window.getEnd() && seg.getEndTime() >= window.getStart()) {
					ret++;
				}
			}
		}
		return ret;
	}

	@Test
	public void testKeyword() throws IOException {
		PropertyConfigurator.configure("conf/log4j-server2.properties");

		Configuration conf = new Configuration();
		conf.load("conf/index_twitter_lsmi.conf");

		LSMTInvertedIndex index = new LSMTInvertedIndex(conf);
		index.init();

		int ts = 606602;
		int te = 696622;
		Interval window = new Interval(0, ts, te, 0);
		System.out.println("begin to verify");
		System.out.flush();
		// 遍历所有的posting list
		try {
			for (SSTableMeta meta : index.getVersion().diskTreeMetas) {
				ListDiskBDBSSTableReader reader = (ListDiskBDBSSTableReader) index.getSSTableReader(index.getVersion(),
						meta);
				WritableComparable key = new StringKey("#beatcancer");
				int expect = ExpectedResult(key, reader, window);
				int answer = queryResult(key, reader, window);
				// fos.close();
				System.out.println("expected size:" + expect + "," + expect);
				Assert.assertEquals(expect, answer);
			}

		} finally {
			index.close();
		}
	}

	@Test
	public void test() throws IOException {
		PropertyConfigurator.configure("conf/log4j-server2.properties");

		Configuration conf = new Configuration();
		conf.load("conf/index_twitter_lsmi.conf");

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
				ListDiskBDBSSTableReader reader = (ListDiskBDBSSTableReader) index.getSSTableReader(index.getVersion(),
						meta);
				Iterator<WritableComparable> iter = reader.keySetIter();
				while (iter.hasNext()) {
					WritableComparable key = iter.next();
					int expect = ExpectedResult(key, reader, window);
					int answer = queryResult(key, reader, window);
					// fos.close();
					System.out.println("expected size:" + expect + "," + expect);
					Assert.assertEquals(expect, answer);
				}
				((BDBBtree.BDBKeyIterator) iter).close();
			}
		} finally {
			index.close();
		}
	}
}
