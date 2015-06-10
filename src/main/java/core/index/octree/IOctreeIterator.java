package core.index.octree;

import java.util.Iterator;

public interface IOctreeIterator extends Iterator<OctreeNode> {
	public void addNode(OctreeNode node);
}
