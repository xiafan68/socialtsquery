package core.index.octree;

import java.io.IOException;

import org.junit.Assert;
import org.junit.Test;

import common.MidSegment;

import core.commom.Encoding;
import core.index.octree.MemoryOctree.OctreeMeta;
import fanxia.file.DirLineReader;

public class MemoryOctreeIterTest {
	@Test
	public void test() throws IOException {
		DirLineReader reader = new DirLineReader(
				"/Users/xiafan/Documents/dataset/expr/twitter/twitter_segs");
		String line = null;
		int i = 0;
		MemoryOctree octree = new MemoryOctree(new OctreeMeta());
		while (null != (line = reader.readLine())) {
			MidSegment seg = new MidSegment();
			seg.parse(line);
			octree.insert(seg.getPoint(), seg);
			if (i++ >= 100000) {
				break;
			}
		}
		reader.close();
		MemoryOctreeIterator iter = new MemoryOctreeIterator(octree);
		Encoding pre = null;
		int size = 0;
		while (iter.hasNext()) {
			OctreeNode curNode = iter.next();
			size += curNode.size();
			System.out.println(curNode);
			Encoding cur = curNode.getEncoding();
			if (pre != null) {
				Assert.assertTrue(pre.compareTo(cur) < 0);
				Assert.assertTrue(pre.getZ() + pre.getEdgeLen() >= cur.getZ()
						+ cur.getEdgeLen());
			}
			pre = cur;
		}
		System.out.println(size);
		Assert.assertTrue(size == 100001);
	}
}
