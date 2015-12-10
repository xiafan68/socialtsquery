package core.executor.domain;

import java.util.Collections;
import java.util.Comparator;

import common.MidSegment;
import core.executor.ExecContext;

public class AndMergedMidSeg extends MergedMidSeg {
	private short hittedKeys = 0;
	private int curMin = Integer.MAX_VALUE;

	public AndMergedMidSeg(ExecContext ctx) {
		super(ctx);
	}

	public AndMergedMidSeg(AndMergedMidSeg other) {
		super(other.ctx);
		hittedKeys = other.hittedKeys;
	}

	@Override
	public MergedMidSeg addMidSeg(int keyIdx, MidSegment seg, float weight) {
		if (weights[keyIdx] < 0) {
			hittedKeys++;
		}
		weights[keyIdx] = weight;
		AndMergedMidSeg ret = new AndMergedMidSeg(this);
		int idx = Collections.binarySearch(segList, seg, new Comparator<MidSegment>() {
			public int compare(MidSegment arg0, MidSegment arg1) {
				return Integer.compare(arg0.getStart(), arg1.getStart());
			}
		});
		if (idx < 0) {
			curMin = Math.min(curMin, seg.getStartCount());
			curMin = Math.min(curMin, seg.getEndCount());

			idx = Math.abs(idx) - 1;
			segList.add(idx, seg);
			ret.computeScore();
		}
		return ret;
	}

	@Override
	public void addMidSegNoCopy(int keyIdx, MidSegment seg, float weight) {
		if (weights[keyIdx] < 0) {
			hittedKeys++;
		}
		weights[keyIdx] = weight;
		int idx = Collections.binarySearch(segList, seg, new Comparator<MidSegment>() {
			public int compare(MidSegment arg0, MidSegment arg1) {
				return Integer.compare(arg0.getStart(), arg1.getStart());
			}
		});
		if (idx < 0) {
			curMin = Math.min(curMin, seg.getStartCount());
			curMin = Math.min(curMin, seg.getEndCount());
			idx = Math.abs(idx) - 1;
			segList.add(idx, seg);
			computeScore();
		}
	}

	/**
	 * 计算该微博的 bestScore 和 worstScore
	 * 
	 * @return true如果当前对象的上下界有所改变
	 */
	public boolean computeScore() {
		if (hittedKeys == -1)
			return false;

		// 这里使用所有一出现的posting list的最小值当做当前对象的最大值，因为它在这些列表中度出现
		double postTopValue = Double.MAX_VALUE;
		for (int i = 0; i < weights.length; i++) {
			if (weights[i] != -1f)
				postTopValue = Math.min(postTopValue, ctx.getBestScore(i));
		}

		// 当前item绝不可能出现
		if (curMin > postTopValue) {
			hittedKeys = -1;
			bestscore = IMPOSSIBLE_VALUE;
			worstscore = IMPOSSIBLE_VALUE;
			return true;
		}

		float preBScore = bestscore;
		float preWScore = worstscore;

		bestscore = 0;
		worstscore = 0;
		int segScore = 0;

		int hitInv = 0;
		int preXPoint = 0;
		float preValue = 0f;
		for (MidSegment seg : segList) {
			segScore += seg.getValue(); // 计算seg内的分值累积和，不包含最后一个点
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
		worstscore = segScore;

		// 计算对于已经遇到的keywords，当前元素分值还未知的区间段
		int margin = Math.max(0, ctx.getQuery().getEndTime() - endTime);
		margin += Math.max(0, startTime - ctx.getQuery().getEndTime());
		int intern = endTime - startTime + 1;
		int remains = ctx.getLifeTimeBound()[1] - intern;
		margin = Math.min(margin, remains);
		int unHitInv = intern - hitInv + margin;

		bestscore = worstscore;
		bestscore += unHitInv * postTopValue;
		if (hittedKeys != weights.length) {
			worstscore = INIT_VALUE;
		}
		return Float.compare(preBScore, bestscore) != 0 || Float.compare(preWScore, worstscore) != 0;
	}

	public boolean validAnswer() {
		return hittedKeys == weights.length;
	}

	@Override
	public boolean equals(Object o) {
		if (o instanceof WeightedMergedMidSeg)
			return getMid() == ((WeightedMergedMidSeg) o).getMid();
		else
			return false;
	}

}
