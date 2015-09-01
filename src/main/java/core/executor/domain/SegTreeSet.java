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

//import com.sun.scenario.effect.Merge;

/**
 * Created by teisei on 15-3-24.
 */
public class SegTreeSet extends ISegQueue {
	private static final Logger logger = LoggerFactory
			.getLogger(SegTreeSet.class);
	/**
	 * 使用红黑树
	 */
	protected TreeSet<MergedMidSeg> pOrderQueue;
	protected TreeSet<MergedMidSeg> sOrderQueue;

	public SegTreeSet(Comparator comparator, Comparator sOrder) {
		pOrderQueue = new TreeSet<MergedMidSeg>(comparator);
		sOrderQueue = new TreeSet<MergedMidSeg>(sOrder);
	}

	/**
	 * 获得堆顶对象。
	 * 
	 * @return
	 */
	public MergedMidSeg peek() {
		return pOrderQueue.first();
		// return priorityQueue.peek();
	}

	/**
	 * 移除bestScore最小的一个元素，更新worstScore。
	 */
	public void poll() {
		// MergedMidSeg seg = priorityQueue.poll();
		MergedMidSeg seg = pOrderQueue.pollFirst();
		sOrderQueue.remove(seg);
		while (seg != null && !seg.computeScore()) {
			pOrderQueue.add(seg);
			sOrderQueue.add(seg);
			seg = pOrderQueue.pollFirst();
			sOrderQueue.remove(seg);
		}
	}

	/**
	 * used by topk queue
	 * 
	 * @return
	 */
	public float getMinWorstScore() {
		float ret = Float.MIN_VALUE;
		// if (!priorityQueue.isEmpty())
		// ret = priorityQueue.peek().getWorstscore();
		if (!pOrderQueue.isEmpty()) {
			while (true) {
				MergedMidSeg cur = pOrderQueue.pollFirst();
				sOrderQueue.remove(cur);
				boolean state = cur.computeScore();
				pOrderQueue.add(cur);
				sOrderQueue.add(cur);
				if (!state) {
					ret = cur.getWorstscore();
					break;
				}
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
		return minBestScore;
	}

	/**
	 * used by cand queue
	 * 
	 * @return
	 */
	public float getMaxBestScore() {
		float ret = Float.MIN_VALUE;
		// if (!priorityQueue.isEmpty())
		// ret = priorityQueue.peek().getBestscore();
		if (!pOrderQueue.isEmpty()) {
			while (true) {
				MergedMidSeg cur = pOrderQueue.first();
				pOrderQueue.remove(cur);
				boolean state = cur.computeScore();
				pOrderQueue.add(cur);
				if (!state) {
					ret = cur.getBestscore();
					break;
				}
			}
		}
		return ret;
	}

	public boolean contains(MergedMidSeg seg) {
		if (seg == null)
			return false;
		return pOrderQueue.contains(seg);
		// return priorityQueue.contains(seg);
	}

	public void update(MergedMidSeg preSeg, MergedMidSeg newSeg) {
		if (preSeg != null)
			pOrderQueue.remove(preSeg);
		// priorityQueue.remove(preSeg);
		pOrderQueue.add(newSeg);

	}

	public boolean isEmpty() {
		return pOrderQueue.isEmpty();
		// return priorityQueue.isEmpty();
	}

	public Iterator<MidSegment> getIter() {
		List<MidSegment> res = new ArrayList<MidSegment>();
		// for (Iterator iter = priorityQueue.iterator(); iter.hasNext();) {
		// MergedMidSeg t = (MergedMidSeg) iter.next();
		// res.add(t.segList.get(0));
		// }
		for (Iterator iter = pOrderQueue.iterator(); iter.hasNext();) {
			MergedMidSeg t = (MergedMidSeg) iter.next();
			res.add(t.segList.get(0));
		}
		return res.iterator();
	}

	public Iterator<MergedMidSeg> iterator() {
		return pOrderQueue.iterator();
		// return priorityQueue.iterator();
	}

	public void remove(MergedMidSeg preSeg) {
		pOrderQueue.remove(preSeg);
		// priorityQueue.remove(preSeg);
	}

	public int size() {
		return pOrderQueue.size();
		// return priorityQueue.size();
	}

	public void printTop() {
		System.out.println("*************");
		// for (Iterator iter = priorityQueue.iterator(); iter.hasNext();) {
		// MergedMidSeg t = (MergedMidSeg) iter.next();
		// System.out.println(t.toString());
		// }
		for (Iterator iter = pOrderQueue.iterator(); iter.hasNext();) {
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

			MidSegment seg = new MidSegment(mid, new Segment(startTime,
					startCount, endTime, endCount));

			/*
			 * 将MidSegment加入堆中
			 */
			// que.update(null, seg);

			que.printTop();

		}
	}

}
