package core.executor.domain;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import common.MidSegment;
import segmentation.Segment;
import util.KeyedPriorityQueue;
import util.MyFile;

public class KeyedTopKQueue {
	private static final Logger logger = LoggerFactory.getLogger(KeyedTopKQueue.class);
	protected KeyedPriorityQueue<Long, MergedMidSeg> worstQueue = new KeyedPriorityQueue<Long, MergedMidSeg>(
			SortWorstscore.INSTANCE);

	/**
	 * 获得堆顶对象。
	 * 
	 * @return
	 */
	public MergedMidSeg peek() {
		refreshWorstScore();
		return worstQueue.first();
	}

	/**
	 * worstscore只会越来越大，所以只需要从最小的往最大的开始更新就可以了
	 * note:其实这里worstscore不需要重新计算，因为只有遇到新的seg才会更新，而这时在插入的时候已经会跟新了
	 */
	private void refreshWorstScore() {

	}

	/**
	 * 移除bestScore最小的一个元素，更新worstScore。
	 */
	public MergedMidSeg poll() {
		refreshWorstScore();
		return worstQueue.poll();
	}

	/**
	 * used by topk queue
	 * 
	 * @return
	 */
	public float getMinWorstScore() {
		float ret = MergedMidSeg.INIT_VALUE + 1;
		refreshWorstScore();
		if (!worstQueue.isEmpty()) {
			MergedMidSeg cur = worstQueue.first();
			ret = cur.getWorstscore();
		}
		return ret;
	}

	public boolean contains(MergedMidSeg seg) {
		if (seg == null)
			return false;
		return worstQueue.contains(seg.getMid());
	}

	public void update(MergedMidSeg seg) {
		// 由于worstscore只会变大，所以它会沉下去
		worstQueue.updateFromTop(seg.getMid());
	}

	public void add(MergedMidSeg seg) {
		worstQueue.offer(seg.getMid(), seg);
	}

	public boolean isEmpty() {
		return worstQueue.isEmpty();
	}

	public Iterator<MidSegment> getIter() {
		List<MidSegment> res = new ArrayList<MidSegment>();
		while (!worstQueue.isEmpty()) {
			MergedMidSeg t = worstQueue.poll();
			res.add(t.segList.get(0));
		}
		return res.iterator();
	}

	public Iterator<MergedMidSeg> iterator() {
		return worstQueue.iterator();
	}

	public void remove(MergedMidSeg preSeg) {
		worstQueue.remove(preSeg.getMid());
	}

	public int size() {
		return worstQueue.size();
	}

	public void printTop() {
		System.out.println("*************");
		// for (Iterator iter = priorityQueue.iterator(); iter.hasNext();) {
		// MergedMidSeg t = (MergedMidSeg) iter.next();
		// System.out.println(t.toString());
		// }
		for (Iterator iter = worstQueue.iterator(); iter.hasNext();) {
			MergedMidSeg t = (MergedMidSeg) iter.next();
			System.out.println(t.toString());
		}
	}
}
