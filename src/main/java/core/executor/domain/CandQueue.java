package core.executor.domain;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.TreeSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import Util.MyFile;
import common.MidSegment;
import segmentation.Segment;

/**
 * 按照bestscore从大到小排序
 * 
 * @author xiafan
 *
 */
public class CandQueue {
	private static final Logger logger = LoggerFactory.getLogger(CandQueue.class);
	/**
	 * 使用红黑树
	 */
	protected TreeSet<MergedMidSeg> bestQueue = new TreeSet<MergedMidSeg>(SortBestscore.INSTANCE);

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
			MergedMidSeg seg = bestQueue.pollFirst();
			while (seg != null && seg.computeScore()) {
				bestQueue.add(seg);
				seg = bestQueue.pollFirst();
			}
			if (!bestQueue.contains(seg)) {
				bestQueue.add(seg);
			}
		}
	}

	/**
	 * 移除bestScore最小的一个元素，更新worstScore。
	 */
	public void poll() {
		refreshScore();
		MergedMidSeg seg = bestQueue.pollFirst();
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
		return bestQueue.contains(seg);
	}

	public void update(MergedMidSeg preSeg, MergedMidSeg newSeg) {
		if (preSeg != null) {
			bestQueue.remove(preSeg);
		}
		bestQueue.add(newSeg);
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
		bestQueue.remove(preSeg);
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

	public static void main(String args[]) throws IOException {
		int start = 2;
		int end = 7;
		/*
		 * 创建一个按照bestscore降序的堆
		 */
		CandQueue que = new CandQueue();
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
