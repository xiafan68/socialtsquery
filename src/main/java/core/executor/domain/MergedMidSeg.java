package core.executor.domain;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import common.MidSegment;
import core.executor.ExecContext;

public abstract class MergedMidSeg {
	List<MidSegment> segList = new ArrayList<MidSegment>();
	ExecContext ctx;
	float[] weights; // item相关的weight
	protected float bestscore = 0;
	protected float worstscore = 0;

	public MergedMidSeg(ExecContext ctx) {
		this.ctx = ctx;
		weights = new float[ctx.getQuery().keywords.length];
		Arrays.fill(weights, -1f);
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
	public abstract MergedMidSeg addMidSeg(int keyIdx, MidSegment seg, float weight);

	/**
	 * 直接更新当前实例
	 * 
	 * @param keyIdx
	 * @param seg
	 * @param weight
	 */
	public abstract void addMidSegNoCopy(int keyIdx, MidSegment seg, float weight);

	/**
	 * 计算该微博的 bestScore 和 worstScore
	 * 
	 * @return true如果当前对象的上下界有所改变
	 */
	public abstract boolean computeScore();

	public abstract boolean validAnswer();

	public float getBestscore() {
		return bestscore;
	}

	public float getWorstscore() {
		return worstscore;
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

}
