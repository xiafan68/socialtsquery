package core.executor.domain;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import common.MidSegment;
import util.KeyedPriorityQueue;

public class KeyedCandQueue {
	private static final Logger logger = LoggerFactory.getLogger(KeyedCandQueue.class);
	protected KeyedPriorityQueue<Long, MergedMidSeg> bestQueue = new KeyedPriorityQueue<Long, MergedMidSeg>(
			SortBestscore.INSTANCE);

	/**
	 * 获得堆顶对象。
	 * 
	 * @return
	 */
	public MergedMidSeg peek() {
		refreshScore();
		return bestQueue.first();
	}

	/**
	 * 由于每个posting list的bestscore发生了变化，这里也需要重新计算每个cand的bestscore
	 */
	private void refreshScore() {
		if (!bestQueue.isEmpty()) {
			MergedMidSeg pre = null;
			while (true) {
				MergedMidSeg seg = bestQueue.first();
				if (seg != pre && seg != null && seg.computeScore()) {
					bestQueue.updateFromTop(seg.getMid());
					pre = seg;
				} else {
					break;
				}
			}
		}
	}

	/**
	 * 移除bestScore最小的一个元素，更新worstScore。
	 */
	public void poll() {
		refreshScore();
		bestQueue.poll();
	}

	/**
	 * used by cand queue
	 * 
	 * @return
	 */
	public float getMaxBestScore() {
		float ret = Float.MIN_VALUE;
		refreshScore();
		if (!bestQueue.isEmpty()) {
			ret = bestQueue.first().getBestscore();
		}
		return ret;
	}

	public boolean contains(MergedMidSeg seg) {
		if (seg == null)
			return false;
		return bestQueue.contains(seg.getMid());
	}

	public void update(MergedMidSeg seg) {
		bestQueue.updateFromTop(seg.getMid());
	}

	public void add(MergedMidSeg mid) {
		bestQueue.offer(mid.getMid(), mid);
	}

	public boolean isEmpty() {
		return bestQueue.isEmpty();
	}

	public Iterator<MidSegment> getIter() {
		List<MidSegment> res = new ArrayList<MidSegment>();
		for (Iterator iter = bestQueue.iterator(); iter.hasNext();) {
			MergedMidSeg t = (MergedMidSeg) iter.next();
			res.add(t.segList.get(0));
		}
		return res.iterator();
	}

	public Iterator<MergedMidSeg> iterator() {
		return bestQueue.iterator();
	}

	public void remove(MergedMidSeg preSeg) {
		bestQueue.remove(preSeg.getMid());
	}

	public int size() {
		return bestQueue.size();
	}

	public void printTop() {
		System.out.println("*************");
		// for (Iterator iter = priorityQueue.iterator(); iter.hasNext();) {
		// MergedMidSeg t = (MergedMidSeg) iter.next();
		// System.out.println(t.toString());
		// }
		for (Iterator iter = bestQueue.iterator(); iter.hasNext();) {
			MergedMidSeg t = (MergedMidSeg) iter.next();
			System.out.println(t.toString());
		}
	}

}