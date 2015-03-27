package core.executor.domain;

//import Util.MyFile;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Queue;

import segmentation.Segment;
import Util.MyFile;
import core.index.MidSegment;

/**
 * 仅仅负责维护items的队列，不涉及items的状态更新逻辑, 用作topk队列时，需要维护整个队列里面的最大值，最小值
 * 
 * @author dingcheng
 * @verion 0.1 2015/2/9.
 * @author xiafan
 * @version 0.2 2015/3/11
 */
public class SegQueue extends ISegQueue{

	/**
	 * 优先队列
	 */
	protected Queue<MergedMidSeg> priorityQueue;
	protected float minBestScore = Float.MAX_VALUE;
	boolean isTopkQueue = false;

	public SegQueue(Comparator comparator, boolean topk) {
		priorityQueue = new PriorityQueue<MergedMidSeg>(11, comparator);
		isTopkQueue = topk;
	}

	/**
	 * 获得堆顶对象。
	 * 
	 * @return
	 */
	public MergedMidSeg peek() {
		return priorityQueue.peek();
	}

	/**
	 * 移除bestScore最小的一个元素，更新worstScore。
	 */
	public void poll() {
		MergedMidSeg seg = priorityQueue.poll();
		// 更新最小bestscore
		if (isTopkQueue && seg != null && seg.getBestscore() == minBestScore) {
			minBestScore = Float.MAX_VALUE;
			Iterator<MergedMidSeg> iter = priorityQueue.iterator();
			while (iter.hasNext()) {
				MergedMidSeg cur = iter.next();
				if (minBestScore > cur.getBestscore()) {
					minBestScore = cur.getBestscore();
				}
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
		if (!priorityQueue.isEmpty())
			ret = priorityQueue.peek().getWorstscore();
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
		if (!priorityQueue.isEmpty())
			ret = priorityQueue.peek().getBestscore();
		return ret;
	}

	public boolean contains(MergedMidSeg seg){
		return priorityQueue.contains(seg);
	}
	public void update(MergedMidSeg preSeg, MergedMidSeg newSeg) {
		if (preSeg != null)
			priorityQueue.remove(preSeg);
		priorityQueue.offer(newSeg);
		if (isTopkQueue && minBestScore > newSeg.getBestscore()) {
			minBestScore = newSeg.getBestscore();
		}
	}

	public boolean isEmpty() {
		return priorityQueue.isEmpty();
	}

	public Iterator<MidSegment> getIter() {
		List<MidSegment> res = new ArrayList<MidSegment>();
		for (Iterator iter = priorityQueue.iterator(); iter.hasNext();) {
			MergedMidSeg t = (MergedMidSeg) iter.next();
			res.add(t.segList.get(0));
		}
		return res.iterator();
	}

	public Iterator<MergedMidSeg> iterator() {
		return priorityQueue.iterator();
	}

	public void remove(MergedMidSeg preSeg) {
		priorityQueue.remove(preSeg);
	}

	public int size() {
		return priorityQueue.size();
	}

	public void printTop() {
		System.out.println("*************");
		for (Iterator iter = priorityQueue.iterator(); iter.hasNext();) {
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
		SegQueue que = new SegQueue(new SortBestscore(), false);
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
			//que.update(null, seg);

			que.printTop();

		}
	}

}
