package core.index.octree;

import java.util.Comparator;
import java.util.PriorityQueue;

public class OctreeIterator implements IOctreeIterator {

	PriorityQueue<OctreeNode> traverseQueue = new PriorityQueue<OctreeNode>(
			256, new Comparator<OctreeNode>() {
				@Override
				public int compare(OctreeNode o1, OctreeNode o2) {
					return o1.getEncoding().compareTo(o2.getEncoding());
				}
			});

	public OctreeIterator(MemoryOctree tree) {
		if (tree.root != null)
			traverseQueue.offer(tree.root);
	}

	public OctreeIterator(OctreeNode root) {
		if (root != null)
			traverseQueue.offer(root);
	}

	@Override
	public void addNode(OctreeNode node) {
		traverseQueue.offer(node);
	}

	@Override
	public boolean hasNext() {
		return !traverseQueue.isEmpty();
	}

	@Override
	public OctreeNode next() {
		while (!traverseQueue.isEmpty()) {
			OctreeNode node = traverseQueue.poll();
			if (node.isLeaf()) {
				return node;
			} else {
				for (int i = 0; i < 8; i++) {
					traverseQueue.add(node.getChild(i));
				}
			}
		}
		return null;
	}

	@Override
	public void remove() {
		throw new RuntimeException(
				"remove is not implemented for OctreeIterator!!!");
	}

}
