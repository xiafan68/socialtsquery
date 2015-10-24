package core.executor.domain;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.TreeSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import common.MidSegment;

import segmentation.Segment;
import Util.MyFile;

/**
 * TOPK: 需要知道最小worst，会poll最小worst，需要知道最小best
 * cand：需要知道最大best,是否需要最大worst？
 */

/**
 * Created by teisei on 15-3-24.
 */
public class SegTreeSet extends ISegQueue {
	private static final Logger logger = LoggerFactory.getLogger(SegTreeSet.class);
	/**
	 * 使用红黑树
	 */
	protected TreeSet<MergedMidSeg> worstQueue;
	protected TreeSet<MergedMidSeg> bestQueue;
	boolean topk = false;

	public SegTreeSet(Comparator comparator, Comparator sOrder, boolean topk) {
		worstQueue = new TreeSet<MergedMidSeg>(comparator);
		bestQueue = new TreeSet<MergedMidSeg>(sOrder);
		this.topk = topk;
	}

	/**
	 * 获得堆顶对象。
	 * 
	 * @return
	 */
	public MergedMidSeg peek() {
		refreshScore();
		return worstQueue.first();
	}

	private void refreshScore() {
		TreeSet<MergedMidSeg> queueA = worstQueue;
		TreeSet<MergedMidSeg> queueB = bestQueue;
		if (!topk) {
			queueA = bestQueue;
			queueB = worstQueue;
		}
		if (!queueA.isEmpty()) {
			MergedMidSeg seg = queueA.pollFirst();
			queueB.remove(seg);
			while (seg != null && seg.computeScore()) {
				queueA.add(seg);
				queueB.add(seg);
				seg = queueA.pollFirst();
				queueB.remove(seg);
			}
			if (!queueA.contains(seg)) {
				queueA.add(seg);
				queueB.add(seg);
			}
		}
	}

	/**
	 * 移除bestScore最小的一个元素，更新worstScore。
	 */
	public void poll() {
		refreshScore();
		if (topk) {
			MergedMidSeg seg = worstQueue.pollFirst();
			bestQueue.remove(seg);
		} else {
			MergedMidSeg seg = bestQueue.pollFirst();
			if (worstQueue.remove(seg)) {
				logger.error("worstQueue can not find seg " + seg.toString());
			}
		}
	}

	/**
	 * used by topk queue
	 * 
	 * @return
	 */
	public float getMinWorstScore() {
		float ret = Float.MIN_VALUE;
		refreshScore();
		if (!worstQueue.isEmpty()) {
			if (topk) {
				MergedMidSeg cur = worstQueue.first();
				ret = cur.getWorstscore();
			}
		}
		return ret;
	}

	/**
	 * used by topk queue
	 * 
	 * @return
	 */
	public float getMinBestScore() {
		float ret = Float.MIN_VALUE;
		refreshScore();
		if (!worstQueue.isEmpty()) {
			if (topk) {
				MergedMidSeg cur = bestQueue.first();
				ret = cur.getBestscore();
			}
		}
		return ret;
	}

	/**
	 * used by cand queue
	 * 
	 * @return
	 */
	public float getMaxBestScore() {
		float ret = Float.MIN_VALUE;
		refreshScore();
		if (!topk && !worstQueue.isEmpty()) {
			ret = bestQueue.first().getBestscore();
		}
		return ret;
	}

	public boolean contains(MergedMidSeg seg) {
		if (seg == null)
			return false;
		return worstQueue.contains(seg);
	}

	public void update(MergedMidSeg preSeg, MergedMidSeg newSeg) {
		if (preSeg != null) {
			worstQueue.remove(preSeg);
			bestQueue.remove(preSeg);
		}
		worstQueue.add(newSeg);
		bestQueue.add(newSeg);
	}

	public boolean isEmpty() {
		return worstQueue.isEmpty();
	}

	public Iterator<MidSegment> getIter() {
		List<MidSegment> res = new ArrayList<MidSegment>();
		for (Iterator iter = worstQueue.iterator(); iter.hasNext();) {
			MergedMidSeg t = (MergedMidSeg) iter.next();
			res.add(t.segList.get(0));
		}
		return res.iterator();
	}

	public Iterator<MergedMidSeg> iterator() {
		return worstQueue.iterator();
	}

	public void remove(MergedMidSeg preSeg) {
		worstQueue.remove(preSeg);
		bestQueue.remove(preSeg);
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

	public static void main(String args[]) throws IOException {
		int start = 2;
		int end = 7;
		/*
		 * 创建一个按照bestscore降序的堆
		 */
		SegQueue que = new SegQueue(SortBestscore.INSTANCE, false);
		MyFile myFile = new MyFile("./data/input", "utf-8");
		String line = null;
		while ((line = myFile.readLine()) != null) {
			/*
			 * 获得一个MidSegment
			 */
			String record[] = line.split("_");
			long mid = Long.parseLong(record[0]);
			int startTime = Integer.parseInt(record[1]);
			int startCount = Integer.parseInt(record[2]);
			int endTime = Integer.parseInt(record[3]);
			int endCount = Integer.parseInt(record[4]);

			MidSegment seg = new MidSegment(mid, new Segment(startTime, startCount, endTime, endCount));

			/*
			 * 将MidSegment加入堆中
			 */
			// que.update(null, seg);

			que.printTop();

		}
	}

}
