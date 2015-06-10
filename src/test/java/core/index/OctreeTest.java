package core.index;

import java.io.File;
import java.io.IOException;

import org.junit.Test;

import segmentation.Segment;
import common.MidSegment;
import core.index.octree.MemoryOctree;
import core.index.octree.OctreeZOrderBinaryWriter;
import core.index.octree.OctreeTextWriter;

public class OctreeTest {
	@Test
	public void serializeTest() throws IOException {
		MemoryOctree tree = new MemoryOctree();
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

		OctreeZOrderBinaryWriter bwriter = new OctreeZOrderBinaryWriter(new File("/tmp"),
				10);
		bwriter.open();
		tree.visit(bwriter);
		bwriter.close();
	}
}
