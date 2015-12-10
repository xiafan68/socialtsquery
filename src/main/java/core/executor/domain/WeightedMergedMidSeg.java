package core.executor.domain;

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;

import common.MidSegment;
import core.executor.ExecContext;

/**
 * 用以在内存中维护每个item已经读到的entries，利用这些信息用于计算当前元素的最好和最差分值
 * 
 * @author dingcheng
 * @version 0.1 2015/2/9.
 * 
 * @author xiafan
 * @version 0.2 2015/03/10
 */
public class WeightedMergedMidSeg extends MergedMidSeg {

	public WeightedMergedMidSeg(ExecContext ctx) {
		super(ctx);
	}

	public WeightedMergedMidSeg(WeightedMergedMidSeg other) {
		super(other.ctx);
		this.ctx = other.ctx;
		weights = other.weights;
		bestscore = other.bestscore;
		worstscore = other.worstscore;
		segList = other.segList;
	}

	@Override
	public WeightedMergedMidSeg addMidSeg(int keyIdx, MidSegment seg, float weight) {
		weights[keyIdx] = weight;
		WeightedMergedMidSeg ret = new WeightedMergedMidSeg(this);
		int idx = Collections.binarySearch(segList, seg, new Comparator<MidSegment>() {
			public int compare(MidSegment arg0, MidSegment arg1) {
				return Integer.compare(arg0.getStart(), arg1.getStart());
			}
		});
		if (idx < 0) {
			idx = Math.abs(idx) - 1;
			segList.add(idx, seg);
		}
		ret.computeScore();
		return ret;
	}

	@Override
	public void addMidSegNoCopy(int keyIdx, MidSegment seg, float weight) {
		weights[keyIdx] = weight;
		int idx = Collections.binarySearch(segList, seg, new Comparator<MidSegment>() {
			public int compare(MidSegment arg0, MidSegment arg1) {
				return Integer.compare(arg0.getStart(), arg1.getStart());
			}
		});
		if (idx < 0) {
			idx = Math.abs(idx) - 1;
			segList.add(idx, seg);
		}
		computeScore();
	}

	/**
	 * 计算该微博的 bestScore 和 worstScore
	 * 
	 * @return true如果当前对象的上下界有所改变
	 */
	@Override
	public boolean computeScore() {
		float preBScore = bestscore;
		float preWScore = worstscore;

		bestscore = 0;
		worstscore = 0;
		int segScore = 0;

		int hitInv = 0;
		int preXPoint = 0;
		float preValue = 0f;
		for (MidSegment seg : segList) {
			segScore += seg.getValue(ctx.getQuery().getStartTime(), ctx.getQuery().getEndTime()); // 计算seg内的分值累积和，不包含最后一个点
			hitInv += seg.getEndTime() - seg.getStart() + 1;
			if (preXPoint == seg.getStart()) {
				segScore -= preValue;
				hitInv -= 1;
			}
			preXPoint = seg.getStart();
			preValue = seg.getStartCount();

		}

		int startTime = segList.get(0).getStart();
		int endTime = segList.get(segList.size() - 1).getEndTime();
		int idx = 0;
		for (float weight : weights) {
			if (weight > 0.0)
				worstscore += segScore * weight * ctx.getWeight(idx);
			idx++;
		}

		// 计算对于已经遇到的keywords，当前元素分值还未知的区间段
		int margin = Math.max(0, ctx.getQuery().getEndTime() - endTime);
		margin += Math.max(0, startTime - ctx.getQuery().getEndTime());
		int intern = endTime - startTime + 1;
		int remains = ctx.getLifeTimeBound()[1] - intern;
		margin = Math.min(margin, remains);
		int unHitInv = intern - hitInv + margin;

		// 对于还未遇到的keyword，目前简单地认为所有的查询区间位置
		int window = Math.min(ctx.getLifeTimeBound()[1], ctx.getQuery().getQueryWidth());
		bestscore = worstscore;

		for (int i = 0; i < weights.length; i++) {
			if (weights[i] > 0)
				bestscore += unHitInv * ctx.getBestScore(i) * ctx.getWeight(i);
			else
				bestscore += window * ctx.getBestScore(i) * ctx.getWeight(i);
		}
		return Float.compare(preBScore, bestscore) != 0 || Float.compare(preWScore, worstscore) != 0;
	}

	@Override
	public boolean equals(Object o) {
		if (o instanceof WeightedMergedMidSeg)
			return getMid() == ((WeightedMergedMidSeg) o).getMid();
		else
			return false;
	}

	@Override
	public String toString() {
		return "MergedMidSeg [segList=" + segList + ", weights=" + Arrays.toString(weights) + ", bestscore=" + bestscore
				+ ", worstscore=" + worstscore + "]";
	}

	@Override
	public boolean validAnswer() {
		return true;
	}
}
