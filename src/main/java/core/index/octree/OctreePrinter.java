package core.index.octree;

import common.MidSegment;

public class OctreePrinter implements OctreeVisitor {
	int indent = 0;
	StringBuffer indentBuf = new StringBuffer();

	@Override
	public void visitLeaf(OctreeNode octreeNode) {
		System.out.println(indentBuf.toString() + "----------------");
		System.out.println(String.format("%scorner:%s;edgeLen:%d",
				indentBuf.toString(), octreeNode.getCornerPoint().toString(),
				octreeNode.getEdgeLen()));
		for (MidSegment seg : octreeNode.getSegs()) {
			System.out.println(seg.toString());
		}
	}

	@Override
	public void visitIntern(OctreeNode octreeNode) {
		System.out.println(indentBuf.toString() + "----------------");
		System.out.println(String.format("%sintern corner:%s;edgeLen:%d",
				indentBuf.toString(), octreeNode.getCornerPoint().toString(),
				octreeNode.getEdgeLen()));
		indentBuf.append(" ");
		indent += 2;
		for (int i = 4; i < 8; i++) {
			octreeNode.getChild(i).visit(this);
		}

		for (int i = 0; i < 4; i++) {
			octreeNode.getChild(i).visit(this);
		}
		indent -= 2;
		indentBuf = indentBuf.delete(0, indent);
	}
}
