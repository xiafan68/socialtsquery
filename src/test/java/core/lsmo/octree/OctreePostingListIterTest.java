package core.lsmo.octree;

import java.io.IOException;
import java.util.Iterator;

import org.apache.commons.collections.map.DefaultedMap;
import org.junit.Assert;
import org.junit.Test;

import common.MidSegment;
import common.TestDataGenerator;
import common.TestDataGeneratorBuilder;
import core.commom.Encoding;
import core.commom.WritableComparable;
import core.lsmo.internformat.InternFormatCommon;
import core.lsmo.internformat.OctreeInternFormatLSMTFactory;
import core.lsmt.IMemTable.SSTableMeta;
import core.lsmt.postinglist.ISSTableReader;
import segmentation.Interval;

public class OctreePostingListIterTest extends InternFormatCommon {

	public int[] ExpectedResult(WritableComparable key, ISSTableReader reader, Interval window) throws IOException {
		int[] ret = new int[] { 0, 0 };
		System.out.println(key);
		Encoding code = null;
		OctreeNode cur = null;
		// FileOutputStream fos = new FileOutputStream("/tmp/visit.log");
		// System.setOut(new PrintStream(fos));
		// 遍历所有的node，找到相关的segs
		IOctreeIterator scanner = (IOctreeIterator) reader.getPostingListScanner(key);
		while (scanner.hasNext()) {
			cur = scanner.nextNode();
			code = cur.getEncoding();
			if (code.getX() <= window.getEnd() && code.getY() + code.getEdgeLen() > window.getStart()) {
				if (!conf.indexLeafOnly() || !Encoding.isMarkupNode(code)) {
					ret[0]++;
					logger.debug(String.format("qualified node %s", code.toString()));
				}
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

	public int[] queryResult(WritableComparable key, ISSTableReader reader, Interval window) throws IOException {
		int[] ret = new int[] { 0, 0 };
		IOctreeIterator scanner = (IOctreeIterator) reader.getPostingListIter(key, window.getStart(), window.getEnd());
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
		DefaultedMap termCounts = new DefaultedMap(0);
		TestDataGeneratorBuilder builder = TestDataGeneratorBuilder.create().setMaxTerm(20).setMaxMidNum(100)
				.setMaxSegNum(100000);
		TestDataGenerator gen = builder.build();

		SSTableMeta meta = new SSTableMeta(0, 0);
		genDiskSSTable(meta, gen, termCounts, 10000000);
		ISSTableReader reader = OctreeInternFormatLSMTFactory.INSTANCE.newSSTableReader(index, meta);
		reader.init();

		Interval window = new Interval(0, 0, 100, 0);
		int step = 300;
		try {
			for (int i = 0; window.getEnd() + i * step < builder.getMaxTime(); i++) {
				window.setStart(i * step);
				window.setEnd(window.getStart() + step);
				Iterator<WritableComparable> iter = reader.keySetIter();
				while (iter.hasNext()) {
					WritableComparable key = iter.next();
					logger.info(String.format("verify keyword %s with window %s", key.toString(), window.toString()));
					int[] expect = ExpectedResult(key, reader, window);
					int[] answer = queryResult(key, reader, window);
					// fos.close();
					logger.info("expected size:" + expect[0] + "," + expect[1]);
					logger.info("actural size:" + answer[0] + "," + answer[1]);
					// Assert.assertEquals(expect[0], answer[0]);
					Assert.assertEquals(expect[1], answer[1]);
				}
			}
		} finally {

		}
	}
}
