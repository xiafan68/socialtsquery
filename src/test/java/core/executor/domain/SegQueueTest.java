package core.executor.domain;

import junit.framework.Assert;

import org.junit.Test;

import common.MidSegment;

import segmentation.Segment;
import core.executor.ExecContext;
import core.executor.domain.MergedMidSeg;
import core.executor.domain.SegQueue;
import core.executor.domain.SortBestscore;
import core.executor.domain.SortWorstscore;

/**
 * 
 * @author xiafan
 * @version 0.1 Mar 11, 2015
 */
public class SegQueueTest {

	/**
	 * 测试SegQueue能够正常更新topk队列,这里主要测试以下三点： 1. 能够添加元素进入队列 2. 能否更新已有元素的分值 3.
	 * 能否正确更新两个分值
	 */
	@Test
	public void updateTopKTest() {
		SegQueue queue = new SegQueue(new SortWorstscore(), true);

		ExecContext ctx = MergedMidSegTest.fakeContext();
		MergedMidSeg a = new MergedMidSeg(ctx);
		a = a.addMidSeg(0, new MidSegment(0, new Segment(1, 10, 3, 12)), 1f);
		queue.update(null, a);
		Assert.assertEquals(queue.getMinWorstScore(), a.getWorstscore());
		Assert.assertEquals(queue.getMinBestScore(), a.getBestscore());

		MergedMidSeg b = new MergedMidSeg(ctx);
		b = b.addMidSeg(0, new MidSegment(1, new Segment(1, 1, 3, 3)), 1f);
		queue.update(null, b);
		Assert.assertEquals(queue.getMinWorstScore(), b.getWorstscore());
		Assert.assertEquals(queue.getMinBestScore(), b.getBestscore());

		Assert.assertEquals(b, queue.peek());
		Assert.assertEquals(2, queue.size());

		MergedMidSeg newB = b.addMidSeg(0, new MidSegment(1, new Segment(5, 10,
				8, 12)), 1f);
		queue.update(b, newB);
		Assert.assertEquals(queue.getMinWorstScore(), a.getWorstscore());
		//这里是b，应为ctx里面假设所有倒排表的当前最小值为10
		Assert.assertEquals(queue.getMinBestScore(), b.getBestscore());
		Assert.assertEquals(a, queue.peek());
		Assert.assertEquals(2, queue.size());
	}

	/**
	 * 测试SegQueue能够正常更新队列
	 */
	@Test
	public void updateCandTest() {
		SegQueue queue = new SegQueue(new SortBestscore(), false);

		ExecContext ctx = MergedMidSegTest.fakeContext();
		MergedMidSeg a = new MergedMidSeg(ctx);
		a = a.addMidSeg(0, new MidSegment(0, new Segment(1, 10, 3, 12)), 1f);
		queue.update(null, a);
		Assert.assertEquals(queue.getMaxBestScore(), a.getBestscore());

		MergedMidSeg b = new MergedMidSeg(ctx);
		b = b.addMidSeg(0, new MidSegment(1, new Segment(1, 1, 3, 3)), 1f);
		queue.update(null, b);
		Assert.assertEquals(queue.getMaxBestScore(), a.getBestscore());

		Assert.assertEquals(a, queue.peek());
		Assert.assertEquals(2, queue.size());

		MergedMidSeg newB = b.addMidSeg(0, new MidSegment(1, new Segment(5, 100,
				8, 120)), 1f);
		queue.update(b, newB);
		Assert.assertEquals(queue.getMaxBestScore(),newB.getBestscore());
		Assert.assertEquals(newB, queue.peek());
		Assert.assertEquals(2, queue.size());
		
		queue.remove(a);
		Assert.assertEquals(newB, queue.peek());
		Assert.assertEquals(1, queue.size());
	}
}
