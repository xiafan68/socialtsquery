package core.index.octree;

import java.io.IOException;
import java.util.Comparator;
import java.util.PriorityQueue;

import Util.Pair;
import core.index.octree.MemoryOctree.OctreeMeta;

/**
 * 用于按照OctreeNode定义的顺序返回节点，和OctreeMerger的逻辑类似
 * 
 * @author xiafan
 *
 */
public class OctreeMergeView implements IOctreeIterator {
	PriorityQueue<Pair<OctreeNode, IOctreeIterator>> mergeQueue = new PriorityQueue<Pair<OctreeNode, IOctreeIterator>>(
			256, new Comparator<Pair<OctreeNode, IOctreeIterator>>() {
				@Override
				public int compare(Pair<OctreeNode, IOctreeIterator> o1, Pair<OctreeNode, IOctreeIterator> o2) {
					return o1.getKey().getEncoding().compareTo(o2.getKey().getEncoding());
				}
			});

	public void addIterator(IOctreeIterator iter) throws IOException {
		if (iter.hasNext()) {
			OctreeNode node = iter.next();
			mergeQueue.offer(new Pair<OctreeNode, IOctreeIterator>(node, iter));
		}
	}

	@Override
	public OctreeMeta getMeta() {
		throw new RuntimeException("getMeta is not supported by OctreeMergeView");
	}

	@Override
	public void addNode(OctreeNode node) {
		throw new RuntimeException("addNode is not supported by OctreeMergeView");
	}

	@Override
	public void open() throws IOException {
	}

	@Override
	public boolean hasNext() throws IOException {
		return !mergeQueue.isEmpty();
	}

	@Override
	public OctreeNode next() throws IOException {
		Pair<OctreeNode, IOctreeIterator> cur = mergeQueue.poll();
		OctreeNode ret = cur.getKey();
		if (cur.getValue().hasNext()) {
			cur.setKey(cur.getValue().next());
			mergeQueue.offer(cur);
		} else {
			cur.getValue().close();
		}
		return ret;
	}

	@Override
	public void close() throws IOException {
		for (Pair<OctreeNode, IOctreeIterator> cur : mergeQueue) {
			cur.getValue().close();
		}
	}

}
