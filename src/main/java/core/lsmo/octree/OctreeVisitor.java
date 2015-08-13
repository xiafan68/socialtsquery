package core.lsmo.octree;


public interface OctreeVisitor {
	public void visitLeaf(OctreeNode octreeNode);

	public void visitIntern(OctreeNode octreeNode);

}
