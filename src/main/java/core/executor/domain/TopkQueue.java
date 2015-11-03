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

public class TopkQueue {
	private static final Logger logger = LoggerFactory.getLogger(TopkQueue.class);
	/**
	 * 使用红黑树
	 */
	protected TreeSet<MergedMidSeg> worstQueue = new TreeSet<MergedMidSeg>(SortWorstscore.INSTANCE);

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
	 */
	private void refreshWorstScore() {
		if (!worstQueue.isEmpty()) {
			MergedMidSeg seg = worstQueue.pollFirst();
			while (seg != null && seg.computeScore()) {
				worstQueue.add(seg);
				seg = worstQueue.pollFirst();
			}
			if (!worstQueue.contains(seg)) {
				worstQueue.add(seg);
			}
		}
	}

	/**
	 * 移除bestScore最小的一个元素，更新worstScore。
	 */
	public void poll() {
		refreshWorstScore();
		MergedMidSeg seg = worstQueue.pollFirst();
	}

	/**
	 * used by topk queue
	 * 
	 * @return
	 */
	public float getMinWorstScore() {
		float ret = Float.MIN_VALUE;
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
		return worstQueue.contains(seg);
	}

	public void update(MergedMidSeg preSeg, MergedMidSeg newSeg) {
		if (preSeg != null) {
			worstQueue.remove(preSeg);
		}
		worstQueue.add(newSeg);
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
		TopkQueue que = new TopkQueue();
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
