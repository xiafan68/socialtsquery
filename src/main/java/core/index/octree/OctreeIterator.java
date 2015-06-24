package core.index.octree;

import java.io.IOException;
import java.util.Comparator;
import java.util.PriorityQueue;

import core.index.octree.MemoryOctree.OctreeMeta;

/**
 * an iterator visiting a memory octree
 * @author xiafan
 *
 */
public class OctreeIterator implements IOctreeIterator {
	OctreeMeta meta;
	PriorityQueue<OctreeNode> traverseQueue = new PriorityQueue<OctreeNode>(
			256, new Comparator<OctreeNode>() {
				@Override
				public int compare(OctreeNode o1, OctreeNode o2) {
					return o1.getEncoding().compareTo(o2.getEncoding());
				}
			});

	public OctreeIterator(MemoryOctree tree) {
		meta = tree.getMeta();
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
