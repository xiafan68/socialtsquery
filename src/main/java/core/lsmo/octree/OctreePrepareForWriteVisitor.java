package core.lsmo.octree;

public enum OctreePrepareForWriteVisitor implements OctreeVisitor {
	INSTANCE;
	public float splitingRatio = 2;

	@Override
	public void visitLeaf(OctreeNode octreeNode) {
	/*	if (octreeNode.getCornerPoint().getZ() == 8 && octreeNode.getCornerPoint().getX() == 696968
				&& octreeNode.getCornerPoint().getY() == 696968) {
			System.out.println("visitLeaf");
		}*/
		if (octreeNode.size() != 0) {
			int[] hist = octreeNode.histogram();
			if (octreeNode.getEdgeLen() > 1
					&& (hist[1] == 0 || ((float) hist[0] + 1) / (hist[1] + 1) > splitingRatio)) {
				octreeNode.split();
				octreeNode.visit(this);
			}
		}
	}

	@Override
	public void visitIntern(OctreeNode octreeNode) {
		for (int i = 0; i < 8; i++)
			octreeNode.getChild(i).visit(this);
	}

}
