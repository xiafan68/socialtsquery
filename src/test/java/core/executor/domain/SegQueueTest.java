package core.executor.domain;

import org.junit.Assert;
import org.junit.Test;

import common.MidSegment;
import core.executor.ExecContext;
import segmentation.Segment;

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
		TopkQueue queue = new TopkQueue();

		ExecContext ctx = MergedMidSegTest.fakeContext();
		MergedMidSeg a = new MergedMidSeg(ctx);
		a = a.addMidSeg(0, new MidSegment(0, new Segment(1, 10, 3, 12)), 1f);
		queue.update(null, a);
		Assert.assertTrue(Double.compare(queue.getMinWorstScore(), a.getWorstscore()) == 0);

		MergedMidSeg b = new MergedMidSeg(ctx);
		b = b.addMidSeg(0, new MidSegment(1, new Segment(1, 1, 3, 3)), 1f);
		queue.update(null, b);
		System.out.println(a.getWorstscore() + "," + b.getWorstscore() + "," + queue.getMinWorstScore());
		Assert.assertTrue(Double.compare(queue.getMinWorstScore(), b.getWorstscore()) == 0);

		Assert.assertEquals(b, queue.peek());
		Assert.assertEquals(2, queue.size());

		MergedMidSeg newB = b.addMidSeg(0, new MidSegment(1, new Segment(5, 10, 8, 12)), 1f);
		queue.update(b, newB);
		System.out.println(a.getWorstscore() + "," + newB.getWorstscore() + "," + queue.getMinWorstScore());
		Assert.assertTrue(Double.compare(queue.getMinWorstScore(), a.getWorstscore()) == 0);

		// 这里是b，应为ctx里面假设所有倒排表的当前最小值为10
		Assert.assertEquals(a, queue.peek());
		Assert.assertEquals(2, queue.size());
	}

	/**
	 * 测试SegQueue能够正常更新队列
	 */
	@Test
	public void updateCandTest() {
		CandQueue queue = new CandQueue();

		ExecContext ctx = MergedMidSegTest.fakeContext();
		MergedMidSeg a = new MergedMidSeg(ctx);
		a = a.addMidSeg(0, new MidSegment(0, new Segment(1, 10, 3, 12)), 1f);
		queue.update(null, a);
		Assert.assertTrue(Double.compare(queue.getMaxBestScore(), a.getBestscore()) == 0);

		MergedMidSeg b = new MergedMidSeg(ctx);
		b = b.addMidSeg(0, new MidSegment(1, new Segment(1, 1, 3, 3)), 1f);
		queue.update(null, b);
		System.out.println(a.getBestscore() + "," + b.getBestscore() + "," + queue.getMaxBestScore());
		Assert.assertTrue(Double.compare(queue.getMaxBestScore(), a.getBestscore()) == 0);

		Assert.assertEquals(a, queue.peek());
		Assert.assertEquals(2, queue.size());

		MergedMidSeg newB = b.addMidSeg(0, new MidSegment(1, new Segment(5, 100, 8, 120)), 1f);
		queue.update(b, newB);
		System.out.println(a.getBestscore() + "," + newB.getBestscore() + "," + queue.getMaxBestScore());
		Assert.assertTrue(Double.compare(queue.getMaxBestScore(), newB.getBestscore()) == 0);
		Assert.assertEquals(newB, queue.peek());
		Assert.assertEquals(2, queue.size());

		queue.remove(a);
		Assert.assertEquals(newB, queue.peek());
		Assert.assertEquals(1, queue.size());
	}
}
