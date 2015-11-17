package core.lsmt;

import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

import common.MidSegment;
import util.Pair;

/**
 * 用于按照OctreeNode定义的顺序返回节点，和OctreeMerger的逻辑类似
 * 
 * @author xiafan
 *
 */
public class PostingListMergeView implements IPostingListIterator {
	PriorityQueue<Pair<Pair<Integer, List<MidSegment>>, IPostingListIterator>> mergeQueue = new PriorityQueue<Pair<Pair<Integer, List<MidSegment>>, IPostingListIterator>>(
			256,
			new Comparator<Pair<Pair<Integer, List<MidSegment>>, IPostingListIterator>>() {
				@Override
				public int compare(
						Pair<Pair<Integer, List<MidSegment>>, IPostingListIterator> o1,
						Pair<Pair<Integer, List<MidSegment>>, IPostingListIterator> o2) {
					return o2.getKey().getKey().compareTo(o1.getKey().getKey());
				}
			});

	public void addIterator(IPostingListIterator iter) throws IOException {
		if (iter.hasNext()) {
			Pair<Integer, List<MidSegment>> node = iter.next();
			mergeQueue
					.offer(new Pair<Pair<Integer, List<MidSegment>>, IPostingListIterator>(
							node, iter));
		}
	}

	@Override
	public PostingListMeta getMeta() {
		throw new RuntimeException(
				"getMeta is not supported by OctreeMergeView");
	}

	@Override
	public void open() throws IOException {
	}

	@Override
	public boolean hasNext() throws IOException {
		return !mergeQueue.isEmpty();
	}

	@Override
	public void close() throws IOException {
		for (Pair<Pair<Integer, List<MidSegment>>, IPostingListIterator> cur : mergeQueue) {
			cur.getValue().close();
		}
	}

	@Override
	public Pair<Integer, List<MidSegment>> next() throws IOException {
		Pair<Pair<Integer, List<MidSegment>>, IPostingListIterator> cur = mergeQueue
				.poll();
		Pair<Integer, List<MidSegment>> ret = cur.getKey();
		if (cur.getValue().hasNext()) {
			cur.setKey(cur.getValue().next());
			mergeQueue.offer(cur);
		}
		return ret;
	}

	@Override
	public void skipTo(WritableComparableKey key) throws IOException {
		// TODO Auto-generated method stub
	}
}
