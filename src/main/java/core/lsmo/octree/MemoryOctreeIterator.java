package core.lsmo.octree;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

import common.MidSegment;
import core.lsmt.WritableComparableKey;
import util.Pair;
import core.lsmt.PostingListMeta;

/**
 * an iterator visiting a memory octree
 * 
 * @author xiafan
 *
 */
public class MemoryOctreeIterator implements IOctreeIterator {
	PostingListMeta meta;
	int start = Integer.MIN_VALUE;
	int end = Integer.MAX_VALUE;
	PriorityQueue<OctreeNode> traverseQueue = new PriorityQueue<OctreeNode>(
			256, new Comparator<OctreeNode>() {
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
		if (node.getCornerPoint().getX() <= end
				&& node.getCornerPoint().getY() + node.getEdgeLen() > start)
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
	public OctreeNode nextNode() {
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
	public PostingListMeta getMeta() {
		return meta;
	}

	@Override
	public Pair<Integer, List<MidSegment>> next() throws IOException {
		OctreeNode node = nextNode();
		return new Pair<Integer, List<MidSegment>>(
				node.getEncoding().getTopZ(), new ArrayList<MidSegment>(
						node.getSegs()));
	}

	@Override
	public void skipTo(WritableComparableKey key) throws IOException {
		// TODO Auto-generated method stub
		
	}
}
