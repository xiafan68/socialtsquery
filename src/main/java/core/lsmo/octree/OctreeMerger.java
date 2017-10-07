package core.lsmo.octree;

import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

import common.MidSegment;
import core.lsmt.postinglist.PostingListMeta;
import util.Pair;
import core.commom.Encoding;
import core.commom.WritableComparableKey;

/**
 * merge leaf nodes of two octrees
 * 
 * @author xiafan
 *
 */
public class OctreeMerger implements IOctreeIterator {
	IOctreeIterator lhs;
	IOctreeIterator rhs;
	OctreeNode curNode;
	PostingListMeta meta;
	PriorityQueue<OctreeNode> splittedNodes = new PriorityQueue<OctreeNode>(256, new Comparator<OctreeNode>() {
		@Override
		public int compare(OctreeNode o1, OctreeNode o2) {
			return o1.getEncoding().compareTo(o2.getEncoding());
		}
	});

	public OctreeMerger(IOctreeIterator lhs, IOctreeIterator rhs) {
		this.lhs = lhs;
		this.rhs = rhs;
		if (lhs.getMeta() == null || rhs.getMeta() == null) {
			meta = new PostingListMeta(lhs.getMeta() != null ? lhs.getMeta() : rhs.getMeta());
		} else {
			meta = new PostingListMeta(lhs.getMeta(), rhs.getMeta());
		}
	}

	@Override
	public boolean hasNext() throws IOException {
		return curNode != null || !splittedNodes.isEmpty() || lnode != null || lhs.hasNext() || rnode != null
				|| rhs.hasNext();
	}

	OctreeNode lnode;
	OctreeNode rnode;

	private void nextLNode() throws IOException {
		if (lnode == null && lhs.hasNext())
			lnode = lhs.nextNode();
	}

	private void nextRNode() throws IOException {
		if (rnode == null && rhs.hasNext())
			rnode = rhs.nextNode();
	}

	private void advance() throws IOException {
		while (curNode == null) {
			nextRNode();
			nextLNode();

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
			// 也有可能一个包含另一个
			if (cmp == 0) {
				curNode = lnode;
				curNode.addSegs(rnode.getSegs());
				lnode = null;
				rnode = null;
			} else if (lnode.contains(rnode)) {
				lnode.split();
				for (int i = 0; i < 8; i++)
					if (Encoding.isMarkupNode(lnode.getChild(i).getEncoding()) || lnode.getChild(i).size() > 0)
						lhs.addNode(lnode.getChild(i));
				lnode = null;
			} else if (rnode.contains(lnode)) {
				rnode.split();
				for (int i = 0; i < 8; i++) {
					if (Encoding.isMarkupNode(rnode.getChild(i).getEncoding()) || rnode.getChild(i).size() > 0)
						rhs.addNode(rnode.getChild(i));
				}
				rnode = null;
			} else {
				if (cmp > 0) {
					curNode = rnode;
					rnode = null;
				} else {
					curNode = lnode;
					lnode = null;
				}
			}
		}
	}

	@Override
	public OctreeNode nextNode() throws IOException {
		OctreeNode ret = null;
		advance();
		if (!splittedNodes.isEmpty()) {
			if (curNode != null) {
				int cmp = curNode.getEncoding().compareTo(splittedNodes.peek().getEncoding());
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
	public PostingListMeta getMeta() {
		return meta;
	}

	@Override
	public Pair<Integer, List<MidSegment>> next() throws IOException {
		throw new RuntimeException("next should never be invoked on instance of OctreeMerger");
	}

	@Override
	public void skipTo(WritableComparableKey key) throws IOException {
		throw new RuntimeException("skipto OctreeMerger");
	}
}
