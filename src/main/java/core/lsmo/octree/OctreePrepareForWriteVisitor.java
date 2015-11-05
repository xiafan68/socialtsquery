package core.lsmo.octree;

public enum OctreePrepareForWriteVisitor implements OctreeVisitor {
	INSTANCE;
	@Override
	public void visitLeaf(OctreeNode octreeNode) {
		if (octreeNode.getEdgeLen() > 1 && octreeNode.size() > MemoryOctree.size_threshold * 0.5) {
			int[] hist = octreeNode.histogram();
			// 下半部分是上半部分的两倍
			if (((float) hist[0] + 1) / (hist[1] + 1) > 2f) {
				octreeNode.split();
				octreeNode.visit(this);
			}
		}
	}

	@Override
	public void visitIntern(OctreeNode octreeNode) {
		for (int i = 0; i < 7; i++)
			octreeNode.getChild(i).visit(this);
	}

}
