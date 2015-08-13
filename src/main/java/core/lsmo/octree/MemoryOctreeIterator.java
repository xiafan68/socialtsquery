package core.lsmo.octree;

import java.io.IOException;
import java.util.Comparator;
import java.util.PriorityQueue;

import core.lsmo.octree.MemoryOctree.OctreeMeta;

/**
 * an iterator visiting a memory octree
 * 
 * @author xiafan
 *
 */
public class MemoryOctreeIterator implements IOctreeIterator {
	OctreeMeta meta;
	int start = Integer.MIN_VALUE;
	int end = Integer.MAX_VALUE;
	PriorityQueue<OctreeNode> traverseQueue = new PriorityQueue<OctreeNode>(256, new Comparator<OctreeNode>() {
		@Override
		public int compare(OctreeNode o1, OctreeNode o2) {
			return o1.getEncoding().compareTo(o2.getEncoding());
		}
	});

	public MemoryOctreeIterator(MemoryOctree tree) {
		if (tree != null) {
			meta = tree.getMeta();
			if (tree.root != null)
				traverseQueue.offer(tree.root);
		}
	}

	public MemoryOctreeIterator(MemoryOctree tree, int start, int end) {
		if (tree != null) {
			meta = tree.getMeta();
			this.start = start;
			this.end = end;
			if (tree.root != null && intersect(tree.root)) {
				traverseQueue.offer(tree.root);
			}
		}
	}

	private boolean intersect(OctreeNode node) {
		if (node.getCornerPoint().getX() <= end && node.getCornerPoint().getY() + node.getEdgeLen() > start)
			return true;
		return false;
	}

	public MemoryOctreeIterator(OctreeNode root) {
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
					if (intersect(node.getChild(i)))
						traverseQueue.add(node.getChild(i));
				}
			}
		}
		return null;
	}

	@Override
	public void close() {

	}

	@Override
	public void open() throws IOException {

	}

	@Override
	public OctreeMeta getMeta() {
		return meta;
	}
}
