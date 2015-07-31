package core.index.octree;

import java.io.IOException;
import java.util.Comparator;
import java.util.PriorityQueue;

import core.index.octree.MemoryOctree.OctreeMeta;

/**
 * merge leaf nodes of two octrees
 * @author xiafan
 *
 */
public class OctreeMerger implements IOctreeIterator {
	IOctreeIterator lhs;
	IOctreeIterator rhs;
	OctreeNode curNode;
	OctreeMeta meta;
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
		meta = new OctreeMeta(lhs.getMeta(), rhs.getMeta());
	}

	@Override
	public boolean hasNext() throws IOException {
		return curNode != null || !splittedNodes.isEmpty() || lhs.hasNext()
				|| rhs.hasNext();
	}

	OctreeNode lnode;
	OctreeNode rnode;

	private void nextLNode() throws IOException {
		if (lnode == null && lhs.hasNext())
			lnode = lhs.next();
	}

	private void nextRNode() throws IOException {
		if (rnode == null && rhs.hasNext())
			rnode = rhs.next();
	}

	private void advance() throws IOException {
		while (curNode == null) {
			nextLNode();
			nextRNode();
			if (lnode == null || rnode == null) {
				if (lnode != null) {
					curNode = lnode;
					lnode = null;
				} else {
					curNode = rnode;
					rnode = null;
				}
				break;
			}
			int cmp = lnode.getEncoding().compareTo(rnode.getEncoding());
			if (cmp == 0) {
				curNode = lnode;
				curNode.addSegs(rnode.getSegs());
				lnode = null;
				rnode = null;
			} else if (cmp > 0) {
				if (rnode.contains(lnode)) {
					rnode.split();
					for (int i = 0; i < 8; i++) {
						if (rnode.getChild(i).size() > 0)
							rhs.addNode(rnode.getChild(i));
					}
				} else {
					curNode = rnode;
				}
				rnode = null;
			} else if (cmp < 0) {
				if (lnode.contains(rnode)) {
					lnode.split();
					for (int i = 0; i < 8; i++)
						if (lnode.getChild(i).size() > 0)
							lhs.addNode(lnode.getChild(i));
				} else {
					curNode = lnode;
				}
				lnode = null;
			}
		}
	}

	@Override
	public OctreeNode next() throws IOException {
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
		} else {
			ret = curNode;
			curNode = null;
		}
		return ret;
	}

	@Override
	public void addNode(OctreeNode node) {
		splittedNodes.add(node);
	}

	@Override
	public void close() throws IOException {
		if (lhs != null)
			lhs.close();
		if (rhs != null)
			rhs.close();

	}

	@Override
	public void open() throws IOException {
		lhs.open();
		rhs.open();
	}

	@Override
	public OctreeMeta getMeta() {
		return meta;
	}
}
