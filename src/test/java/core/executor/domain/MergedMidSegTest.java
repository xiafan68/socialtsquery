package core.executor.domain;

import junit.framework.Assert;

import org.junit.Test;

import segmentation.Interval;
import segmentation.Segment;
import core.executor.ExecContext;
import core.executor.domain.MergedMidSeg;
import core.index.MidSegment;
import core.index.TempKeywordQuery;

/**
 * 
 * @author xiafan
 * @version 0.1 Mar 11, 2015
 */
public class MergedMidSegTest {

	public static ExecContext fakeContext() {
		ExecContext ret = new ExecContext(new TempKeywordQuery(new String[] {
				"a", "b" }, new Interval(0, 0, 100000, 0), 10));
		ret.setLifeTimeBound(new int[] { 8, 15 });
		ret.setWeights(new float[] { 1.0f, 1.0f });
		ret.setBestScores(new float[] { 10f, 10f });
		return ret;
	}

	@Test
	public void test() {
		ExecContext ctx = fakeContext();
		MergedMidSeg merge = new MergedMidSeg(ctx);
		MidSegment newSeg = new MidSegment(0, new Segment(1, 10, 3, 12));
		MergedMidSeg newMerge = merge.addMidSeg(0, newSeg, 1f);
		System.out.println(merge);
		System.out.println(newMerge);

		Assert.assertEquals(33.f, newMerge.getWorstscore());
		//15 * 10 + (15 - 3) * 10
		Assert.assertEquals(303.f, newMerge.getBestscore());

		newSeg = new MidSegment(0, new Segment(5, 5, 7, 7));
		newMerge = newMerge.addMidSeg(1, newSeg, 1f);
		System.out.println(newMerge);
		//
		Assert.assertEquals(102.f, newMerge.getWorstscore());
		Assert.assertEquals(282.f, newMerge.getBestscore());
	}
}
