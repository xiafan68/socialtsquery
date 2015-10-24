package core.executor.domain;

import java.util.Collections;
import java.util.Comparator;

import common.MidSegment;
import core.executor.ExecContext;

public class OrMergedMidSeg extends MergedMidSeg {

	public OrMergedMidSeg(ExecContext ctx) {
		super(ctx);
	}

	public OrMergedMidSeg(OrMergedMidSeg other) {
		super(other);
	}

	public float getBestscore() {
		return bestscore;
	}

	public float getWorstscore() {
		return worstscore;
	}

	/* 对于这条微博增加一个窗口内的 segment */
	/**
	 * 为了降低对内存的消耗，新老对象还是公用了segList，但是当前对象的其它状态不会变
	 * 
	 * @param keyIdx
	 *            关键词列表中的小标，用于指代某个关键词
	 * @param seg
	 * @return 返回修改后的MergedMidSeg
	 */
	public OrMergedMidSeg addMidSeg(int keyIdx, MidSegment seg, float weight) {
		weights[keyIdx] = weight;
		OrMergedMidSeg ret = new OrMergedMidSeg(this);
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

	public long getMid() {
		if (segList.size() > 0)
			return segList.get(0).mid;
		else
			return -1;
	}

	public int getStartTime() {
		if (segList.isEmpty())
			return 0;
		return segList.get(0).getStart();
	}

	public int getEndTime() {
		if (segList.isEmpty())
			return 0;
		return segList.get(segList.size() - 1).getEndTime();
	}

	/**
	 * 计算该微博的 bestScore 和 worstScore
	 * 
	 * @return true如果当前对象的上下界有所改变
	 */
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

		// 这里使用所有一出现的posting list的最小值当做当前对象的最大值，因为它在这些列表中度出现
		double postTopValue = Double.MAX_VALUE;
		for (int i = 0; i < weights.length; i++) {
			if (weights[i] != -1f)
				postTopValue = Math.min(postTopValue, ctx.getBestScore(i));
		}
		bestscore += unHitInv * postTopValue;

		return Float.compare(preBScore, bestscore) != 0 || Float.compare(preWScore, worstscore) != 0;
	}

	public boolean validAnswer() {
		boolean ret = false;
		for (float weight : weights) {
			if (weight > 0) {
				ret = true;
				break;
			}
		}
		return ret;
	}

	@Override
	public boolean equals(Object o) {
		if (o instanceof OrMergedMidSeg)
			return getMid() == ((OrMergedMidSeg) o).getMid();
		else
			return false;
	}

}
