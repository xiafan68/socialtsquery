package core.index;

import java.io.IOException;

import org.junit.Test;

import segmentation.Segment;

import common.MidSegment;
import core.lsmo.octree.MemoryOctree;
import core.lsmo.octree.OctreeTextWriter;
import core.lsmo.octree.MemoryOctree.OctreeMeta;

public class OctreeTest {
	@Test
	public void serializeTest() throws IOException {
		MemoryOctree tree = new MemoryOctree(new OctreeMeta());
		long id = 0;
		for (int i = 0; i < 8; i++) {
			for (int j = i; j < 8; j++) {
				MidSegment seg = new MidSegment(id++, new Segment(i, i, j, j));
				tree.insert(seg.getPoint(), seg);
			}
		}
		tree.print();
		OctreeTextWriter writer = new OctreeTextWriter(null, 10);
		tree.visit(writer);
	}
}
