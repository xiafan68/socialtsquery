package core.index;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;

import org.apache.commons.collections.Factory;
import org.junit.Assert;
import org.junit.Test;

import Util.Configuration;
import common.MidSegment;
import core.commom.Encoding;
import core.index.MemTable.SSTableMeta;
import core.index.octree.MemoryOctree;
import core.index.octree.MemoryOctree.OctreeMeta;
import core.index.octree.MemoryOctreeIterator;
import core.index.octree.OctreeMerger;
import core.index.octree.OctreeNode;
import segmentation.Segment;
import xiafan.util.collection.DefaultedPutMap;

public class OctreeMergerTest {
	@Test
	public void mergeTest() throws IOException {
		for (int i = 0; i < 100; i++) {
			MemoryOctree tree1 = insertAndReadTest(17 + i * 10);
			MemoryOctree tree2 = insertAndReadTest(51 + i * 10);
			HashMap<MidSegment, Integer> segs = new HashMap<MidSegment, Integer>();
			DefaultedPutMap<MidSegment, Integer> map = DefaultedPutMap.decorate(segs, new Factory() {
				@Override
				public Object create() {
					return new Integer(0);
				}
			});

			setupAnswers(new MemoryOctreeIterator(tree1), map);
			setupAnswers(new MemoryOctreeIterator(tree2), map);

			OctreeMerger merge = new OctreeMerger(new MemoryOctreeIterator(tree1), new MemoryOctreeIterator(tree2));
			Encoding pre = null;
			while (merge.hasNext()) {
				OctreeNode node = merge.next();
				if (pre != null) {
					Assert.assertTrue(pre.compareTo(node.getEncoding()) < 0);
				}
				pre = node.getEncoding();

				for (MidSegment seg : node.getSegs()) {
					if (segs.containsKey(seg)) {
						segs.put(seg, segs.get(seg) - 1);
						if (segs.get(seg) == 0)
							segs.remove(seg);
					} else {
						Assert.assertTrue(false);
					}
				}
			}
			Assert.assertEquals(0, segs.size());
		}
	}

	private static void setupAnswers(MemoryOctreeIterator iter, Map<MidSegment, Integer> segs) {
		while (iter.hasNext()) {
			OctreeNode node = iter.next();
			for (MidSegment seg : node.getSegs()) {
				segs.put(seg, segs.get(seg) + 1);
			}
		}
	}

	public static MemoryOctree insertAndReadTest(int seed) {
		MemoryOctree octree = new MemoryOctree(new OctreeMeta());
		Random rand = new Random();
		rand.setSeed(seed);
		HashSet<MidSegment> segs = new HashSet<MidSegment>();
		for (int i = 0; i < 100000; i++) {
			int start = Math.abs(rand.nextInt()) % 10000;
			int count = Math.abs(rand.nextInt()) % 200;
			int tgap = Math.abs(rand.nextInt()) % 100;
			int cgap = Math.abs(rand.nextInt()) % 100;
			MidSegment seg = new MidSegment(rand.nextLong(), new Segment(start, count, start + tgap, count + cgap));
			octree.insert(seg.getPoint(), seg);
			segs.add(seg);
		}
		return octree;
	}

	@Test
	public void merge2SStables() throws IOException {
		Configuration conf = new Configuration();
		conf.load("conf/index.conf");
		LSMOInvertedIndex index = new LSMOInvertedIndex(conf);
		DiskSSTableReader lhs = new DiskSSTableReader(index, new SSTableMeta(59, 2));
		lhs.init();
		DiskSSTableReader rhs = new DiskSSTableReader(index, new SSTableMeta(63, 2));
		rhs.init();

		Iterator<Integer> keyIter = lhs.keySetIter();
		int size = 0;
		OctreeNode pre = null;
		while (keyIter.hasNext()) {
			int key = keyIter.next();
			OctreeMerger merge = new OctreeMerger(lhs.getPostingListScanner(key), rhs.getPostingListScanner(key));
			while (merge.hasNext()) {
				OctreeNode cur = merge.next();
				// System.out.println(cur);
				size += cur.size();
				if (pre != null) {
					if (pre.getEncoding().compareTo(cur.getEncoding()) >= 0)
						System.out.println(key + "\n" + pre + "\n" + cur);
					Assert.assertTrue(pre.getEncoding().compareTo(cur.getEncoding()) < 0);
				}
				pre = cur;
			}
		}
		System.out.println(size);
	}

	@Test
	public void merge3SStables() throws IOException {
		Configuration conf = new Configuration();
		conf.load("conf/index.conf");
		LSMOInvertedIndex index = new LSMOInvertedIndex(conf);
		DiskSSTableReader lhs = new DiskSSTableReader(index, new SSTableMeta(32, 0));
		lhs.init();
		DiskSSTableReader rhs = new DiskSSTableReader(index, new SSTableMeta(33, 0));
		rhs.init();

		OctreeMerger merge = new OctreeMerger(lhs.getPostingListScanner(0), rhs.getPostingListScanner(0));
		DiskSSTableReader rrhs = new DiskSSTableReader(index, new SSTableMeta(34, 0));
		rrhs.init();
		OctreeMerger merge3 = new OctreeMerger(merge, rrhs.getPostingListScanner(0));
		while (merge3.hasNext()) {
			OctreeNode cur = merge3.next();
			System.out.println(cur);
		}
	}
}
