package core.lsmo.octree;

import java.io.IOException;
import java.util.HashSet;
import java.util.Random;

import org.junit.Assert;
import org.junit.Test;

import common.MidSegment;
import core.commom.Encoding;
import core.lsmt.postinglist.PostingListMeta;
import io.DirLineReader;
import segmentation.Segment;

public class MemoryOctreeIterTest {
	@Test
	public void insertAndReadTest() {
		for (int i = 0; i < 100; i++)
			insertAndReadTest(31 + i * 11);
	}

	public void insertAndReadTest(int seed) {
		MemoryOctree octree = new MemoryOctree(new PostingListMeta());
		Random rand = new Random();
		rand.setSeed(seed);
		HashSet<MidSegment> segs = new HashSet<MidSegment>();
		for (int i = 0; i < 10000; i++) {
			int start = Math.abs(rand.nextInt()) % 10000;
			int count = Math.abs(rand.nextInt()) % 200;
			int tgap = Math.abs(rand.nextInt()) % 100;
			int cgap = Math.abs(rand.nextInt()) % 100;
			MidSegment seg = new MidSegment(rand.nextLong(), new Segment(start, count, start + tgap, count + cgap));
			octree.insert(seg);
			segs.add(seg);
		}
		MemoryOctreeIterator iter = new MemoryOctreeIterator(octree);
		Encoding pre = null;
		while (iter.hasNext()) {
			OctreeNode node = iter.nextNode();
			if (pre != null) {
				Assert.assertTrue(pre.compareTo(node.getEncoding()) < 0);
			}
			pre = node.getEncoding();

			for (MidSegment seg : node.segs) {
				if (segs.contains(seg)) {
					segs.remove(seg);
				} else {
					Assert.assertTrue(false);
				}
			}
		}
		Assert.assertEquals(0, segs.size());
	}

	@Test
	public void test() throws IOException {
		// "/Users/xiafan/Documents/dataset/expr/twitter/twitter_segs"
		DirLineReader reader = new DirLineReader("/home/xiafan/dataset/twitter/twitter_segs");
		String line = null;
		int i = 0;
		MemoryOctree octree = new MemoryOctree(new PostingListMeta());
		while (null != (line = reader.readLine())) {
			MidSegment seg = new MidSegment();
			seg.parse(line);
			octree.insert(seg);
			if (i++ >= 100000) {
				break;
			}
		}
		reader.close();
		MemoryOctreeIterator iter = new MemoryOctreeIterator(octree);
		Encoding pre = null;
		int size = 0;
		while (iter.hasNext()) {
			OctreeNode curNode = iter.nextNode();
			size += curNode.size();
			System.out.println(curNode);
			Encoding cur = curNode.getEncoding();
			if (pre != null) {
				Assert.assertTrue(pre.compareTo(cur) < 0);
				Assert.assertTrue(pre.getZ() + pre.getEdgeLen() >= cur.getZ() + cur.getEdgeLen());
			}
			pre = cur;
		}
		System.out.println(size);
		Assert.assertTrue(size == 100001);
	}
}
