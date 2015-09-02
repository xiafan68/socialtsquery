package core.executor.domain;

import java.util.Comparator;
import java.util.Iterator;

import org.apache.commons.collections.ComparatorUtils;

import common.MidSegment;

/**
 * 
 * @author xiafan
 * @version 0.1 Mar 25, 2015
 */
public abstract class ISegQueue {
	public abstract MergedMidSeg peek();

	/**
	 * 移除bestScore最小的一个元素，更新worstScore。
	 */
	public abstract void poll();

	/**
	 * used by topk queue
	 * 
	 * @return
	 */
	public abstract float getMinWorstScore();

	/**
	 * used by topk queue
	 * 
	 * @return
	 */
	public abstract float getMinBestScore();

	/**
	 * used by cand queue
	 * 
	 * @return
	 */
	public abstract float getMaxBestScore();

	public abstract boolean contains(MergedMidSeg seg);

	public abstract void update(MergedMidSeg preSeg, MergedMidSeg newSeg);

	public abstract boolean isEmpty();

	public abstract Iterator<MidSegment> getIter();

	public abstract Iterator<MergedMidSeg> iterator();

	public abstract void remove(MergedMidSeg preSeg);

	public abstract int size();

	public abstract void printTop();

	public static ISegQueue create(boolean topk) {
		if (topk)
			return new SegTreeSet(SortWorstscore.INSTANCE,
					SortBestscore.INSTANCE, topk);
		else {
			return new SegTreeSet(
					ComparatorUtils.reversedComparator(SortBestscore.INSTANCE),
					SortWorstscore.INSTANCE, topk);
		}
	}
}
