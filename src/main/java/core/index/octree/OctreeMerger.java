package core.index.octree;

import java.util.Comparator;
import java.util.PriorityQueue;

/**
 * merge leaf nodes of two octrees
 * @author xiafan
 *
 */
public class OctreeMerger implements IOctreeIterator {
	IOctreeIterator lhs;
	IOctreeIterator rhs;
	OctreeNode curNode;
	PriorityQueue<OctreeNode> splittedNodes = new PriorityQueue<OctreeNode>(
			256, new Comparator<OctreeNode>() {
				@Override
				public int compare(OctreeNode o1, OctreeNode o2) {
					return o1.getEncoding().compareTo(o2.getEncoding());
				}
			});

	public OctreeMerger(IOctreeIterator lhs, IOctreeIterator rhs) {
		this.lhs = lhs;
		this.rhs = rhs;
	}

	@Override
	public boolean hasNext() {
		return curNode != null || !splittedNodes.isEmpty() || lhs.hasNext()
				|| rhs.hasNext();
	}

	OctreeNode lnode;
	OctreeNode rnode;

	private void nextLNode() {
		if (lnode == null && lhs.hasNext())
			lnode = lhs.next();
	}

	private void nextRNode() {
		if (rnode == null && rhs.hasNext())
			rnode = rhs.next();
	}

	private void advance() {
		while (curNode == null) {
			nextLNode();
			nextRNode();
			if (lnode == null || rnode == null) {
				curNode = lnode != null ? lnode : rnode;
				break;
			}
			int cmp = lnode.getEncoding().compareTo(rnode.getEncoding());
			if (cmp == 0) {
				// TODO merge two nodes
			} else if (cmp > 0) {
				if (rnode.contains(lnode)) {
					rnode.split();
					for (int i = 0; i < 8; i++) {
						if (rnode.getChild(i).size() > 0)
							rhs.addNode(rnode.getChild(i));
					}
					rnode = null;
				} else {
					curNode = rnode;
					rnode = null;
					break;
				}
			} else if (cmp < 0) {
				if (lnode.contains(rnode)) {
					lnode.split();
					for (int i = 0; i < 8; i++)
						if (lnode.getChild(i).size() > 0)
							lhs.addNode(lnode.getChild(i));
					lnode = null;
				} else {
					curNode = lnode;
					lnode = null;
					break;
				}
			}
		}
	}

	@Override
	public OctreeNode next() {
		OctreeNode ret = null;
		advance();
		if (!splittedNodes.isEmpty()) {
			if (curNode != null) {
				int cmp = curNode.getEncoding().compareTo(
						splittedNodes.peek().getEncoding());
				if (cmp < 0) {
					ret = curNode;
					curNode = null;
				} else {
					assert cmp != 0;
					ret = splittedNodes.poll();
				}
			} else {
				ret = splittedNodes.poll();
			}
		}
		return ret;
	}

	@Override
	public void remove() {
		throw new RuntimeException("remove is not implemented for OctreeMerger");
	}

	@Override
	public void addNode(OctreeNode node) {
		splittedNodes.add(node);
	}
}
