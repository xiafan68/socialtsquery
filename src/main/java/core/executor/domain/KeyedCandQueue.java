package core.executor.domain;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import common.MidSegment;
import util.KeyedPriorityQueue;
import util.Pair;
import util.Profile;
import util.ProfileField;

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
		Profile.instance.start(ProfileField.MAINTAIN_CAND.toString());
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
		Profile.instance.end(ProfileField.MAINTAIN_CAND.toString());
	}

	/**
	 * 及时删除不可能的元素，降低堆操作的代价 TODO:这个优化不大，主要是octant的上界太大
	 * 
	 * @param threshold
	 */
	public void prune(float threshold) {
		Iterator<Pair<Long, MergedMidSeg>> iter = bestQueue.tailIterator();
		while (iter.hasNext()) {
			Pair<Long, MergedMidSeg> rec = iter.next();
			if (rec.getValue().getBestscore() <= threshold) {
				iter.remove();
				Profile.instance.updateCounter("pruned_by_topk");
			} else {
				break;
			}
		}
	}

	/**
	 * 移除bestScore最小的一个元素，更新worstScore。
	 */
	public MergedMidSeg poll() {
		refreshScore();
		return bestQueue.poll();
	}

	/**
	 * used by cand queue
	 * 
	 * @return
	 */
	public float getMaxBestScore() {
		float ret = Float.MAX_VALUE;
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
