package core.lsmo.octree;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.junit.Test;

import segmentation.Segment;
import common.MidSegment;
import core.commom.Encoding;
import core.commom.Point;
import core.lsmo.octree.OctreeNode.CompressedSerializer;

public class OctreeNodeTest {
	@Test
	public void testPersistent() throws IOException {
		OctreeNode.HANDLER = CompressedSerializer.INSTANCE;
		OctreeNode node = new OctreeNode(new Point(0, 0, 0), 16);
		long mid = 0;
		MidSegment seg = new MidSegment(mid, new Segment(0, 0, 0, 0));
		node.insert(seg.getPoint(), seg);
		seg = new MidSegment(mid++, new Segment(1, 0, 3, 2));
		node.insert(seg.getPoint(), seg);
		seg = new MidSegment(mid++, new Segment(4, 2, 5, 2));
		node.insert(seg.getPoint(), seg);

		ByteArrayOutputStream out = new ByteArrayOutputStream();
		DataOutputStream dos = new DataOutputStream(out);
		node.getEncoding().write(dos);
		node.write(dos);

		DataInputStream input = new DataInputStream(new ByteArrayInputStream(
				out.toByteArray()));

		Encoding code = new Encoding();
		code.read(input);
		OctreeNode oNode = new OctreeNode(code, code.getEdgeLen());
		oNode.read(input);
		System.out.println(node);
		System.out.println(oNode);
		
	}
}
