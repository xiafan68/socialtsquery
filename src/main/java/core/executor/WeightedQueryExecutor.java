package core.executor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import common.MidSegment;
import core.commom.TempKeywordQuery;
import core.executor.domain.KeyedCandQueue;
import core.executor.domain.KeyedTopKQueue;
import core.executor.domain.MergedMidSeg;
import core.executor.domain.WeightedMergedMidSeg;
import core.lsmt.IPostingListIterator;
import core.lsmt.LSMTInvertedIndex;
import core.lsmt.PartitionMeta;
import segmentation.Interval;
import util.MyMath;
import util.Pair;
import util.Profile;
import util.ProfileField;

/**
 * 实现一个baseline算法,基于某个partition的索引执行查询 TODO:某个posting
 * list的最大值改变后，需要Lazy的更新cand和topk队列中对首元素的最大值
 * 
 * @author dingcheng
 * 
 * @modify
 * @author xiafan
 * 
 */
public class WeightedQueryExecutor extends IQueryExecutor {

	public WeightedQueryExecutor(LSMTInvertedIndex reader) {
		super(reader);
	}

	PartitionMeta meta;

	/**
	 * 
	 * @param topk
	 *            用于存放当前topk元素的队列
	 * @param_lifetime 当前BaseExecutor只负责处理所有生命周期不大于lifetime的元素
	 * @throws java.io.IOException
	 */
	public void setupQueryContext(KeyedTopKQueue topk, Map<Long, MergedMidSeg> map) throws IOException {
		this.map = map;
		if (topk != null)
			this.topk = topk;
		else
			this.topk = new KeyedTopKQueue();
		cand = new KeyedCandQueue();

		ctx = new ExecContext(query);
		int part = MyMath.getCeil(maxLifeTime);
		meta = new PartitionMeta(part);
		int minLt = (int) Math.pow(2, part - 1);
		ctx.setLifeTimeBound(new int[] { minLt, minLt << 2 - 1 });
		bestScores = new float[query.keywords.length];
		Arrays.fill(bestScores, 1000000);

		ctx.setBestScores(bestScores);

		loadCursors();
	}

	private void loadCursors() throws IOException {
		String[] keywords = query.keywords;
		cursors = new IPostingListIterator[keywords.length];
		List<String> keywordList = Arrays.asList(keywords);
		Map<String, IPostingListIterator> iters = reader.getPostingListIter(keywordList, query.queryInv.getStart(),
				query.queryInv.getEnd());
		/* 为每个词创建索引读取对象 */
		for (int i = 0; i < keywords.length; i++) {
			if (cursors[i] != null) {
				throw new IllegalStateException("cursor is not closed");
			}
			cursors[i] = iters.get(keywords[i]);
		}
	}

	@Override
	public void query(TempKeywordQuery query) throws IOException {
		this.query = query;
		// 通过keyword和window去索引里查询
	}

	@Override
	public boolean isTerminated() throws IOException {
		if (stop) {
			return true;
		} else {
			float sum = 0;
			for (float bestScore : bestScores) {
				sum += bestScore;
			}
			sum *= Math.min(maxLifeTime, query.getEndTime() - query.getStartTime());
			boolean ret = false;
			// 当前partition不可能有cand能够进入topk
			if (cand.getMaxBestScore() <= topk.getMinWorstScore() && sum <= topk.getMinWorstScore()
					&& topk.size() >= ctx.getQuery().k) {
				ret = true;
			} else {
				// 所有的倒排表都已经遍历过了
				ret = true;
				for (int i = 0; i < cursors.length; i++) {
					IPostingListIterator cursor = cursors[i];
					if (cursor == null || !cursor.hasNext()) {
						bestScores[i] = 0;
					} else {
						ret = false;
					}
				}
				if (ret) {
					refreshTopk();
				}
			}
			stop = ret;

			if (stop) {
				Profile.instance.updateCounter(ProfileField.TOPK.toString(), topk.size());
				Profile.instance.updateCounter(ProfileField.CAND.toString(), cand.size());
			}

			return ret;
		}
	}

	private IPostingListIterator nextCursor() throws IOException {
		IPostingListIterator ret = null;
		for (int i = 0; i < cursors.length; i++) {
			curListIdx = (++curListIdx) % cursors.length;
			ret = cursors[curListIdx];
			if (ret == null) {
				continue;
			} else if (!ret.hasNext()) {
				cursors[curListIdx] = null;
				ret = null;
				bestScores[i] = 0;
				// break;
			} else {
				break;
			}
		}
		return ret;
	}

	/**
	 * 
	 * @param idx
	 * @param midseg
	 * @param iWeight
	 * @return true if the current item is a possible topk
	 */
	/*
	 * private boolean updateCandState(int idx, MidSegment midseg, float
	 * iWeight) { MergedMidSeg preSeg = null; MergedMidSeg newSeg = null; Long
	 * mid = midseg.mid;
	 * 
	 * update the boundary, put it in the QueCand if (map.containsKey(mid)) {
	 * preSeg = map.get(mid); newSeg = preSeg.addMidSeg(idx, midseg, iWeight);//
	 * update the merged } else { newSeg = new MergedMidSeg(ctx); newSeg =
	 * newSeg.addMidSeg(idx, midseg, iWeight); } // update the map with the new
	 * mergedseg map.put(mid, newSeg);
	 * 
	 * boolean ret = true; update the topk and cands if (topk.contains(preSeg))
	 * { topk.update(preSeg, newSeg); } else if (topk.size() < query.k ||
	 * newSeg.getWorstscore() > cand.getMaxBestScore()) { topk.update(preSeg,
	 * newSeg); if (topk.size() > query.k) { // 把bestScore最小的一个移除
	 * cand.update(null, topk.peek()); }
	 * 
	 * if (preSeg != null) cand.remove(preSeg); } else if (newSeg.getBestscore()
	 * > topk.getMinWorstScore()) { cand.update(preSeg, newSeg); } else if
	 * (preSeg != null) { cand.remove(preSeg); map.remove(preSeg.getMid()); ret
	 * = false; } else { map.remove(mid); ret = false;
	 * Profile.instance.updateCounter(Profile.WASTED_REC); } return ret; }
	 */

	/**
	 * 
	 * @param idx
	 * @param midseg
	 * @param iWeight
	 * @return true if the current item is a possible topk
	 */
	private boolean updateCandState_intern(int idx, MidSegment midseg, float iWeight) {
		MergedMidSeg seg = null;
		Long mid = midseg.mid;

		/* update the boundary, put it in the QueCand */
		if (map.containsKey(mid)) {
			seg = map.get(mid);
			seg.addMidSegNoCopy(idx, midseg, iWeight);// update the merged
		} else {
			seg = new WeightedMergedMidSeg(ctx);
			seg.addMidSegNoCopy(idx, midseg, iWeight);
			map.put(mid, seg);
		}
		boolean ret = true;
		/* update the topk and cands */
		if (topk.contains(seg)) {
			topk.update(seg);
		} else if (topk.size() < query.k || seg.getWorstscore() > topk.getMinWorstScore()) {
			topk.add(seg);
			if (topk.size() > query.k) {
				// 把bestScore最小的一个移除
				cand.add(topk.peek());
				topk.poll();
			}

			if (cand.contains(seg))
				cand.remove(seg);
		} else if (seg.getBestscore() > topk.getMinWorstScore()) {
			if (cand.contains(seg))
				cand.update(seg);
			else {
				cand.add(seg);
			}
		} else {
			if (cand.contains(seg))
				cand.remove(seg);
			map.remove(seg.getMid());
			ret = false;
			Profile.instance.updateCounter(ProfileField.WASTED_REC.toString());
		}
		return ret;
	}

	private void refreshTopk() {
		cand.getMaxBestScore();
		while (!cand.isEmpty()) {
			MergedMidSeg seg = cand.peek();
			cand.poll();

			if (seg.getWorstscore() > topk.getMinWorstScore() && seg.getWorstscore() > cand.getMaxBestScore()) {
				cand.add(topk.peek());
				topk.poll();
				topk.add(seg);
			} else if (seg.getBestscore() <= topk.getMinWorstScore()) {
				break;
			}
		}
	}

	@Override
	public boolean advance() throws IOException {
		if (isTerminated()) {
			// 所有倒排记录都读完了。
			// topk cand
			return false;
		}

		IPostingListIterator plc = nextCursor();
		Pair<Integer, List<MidSegment>> node = plc.next();// 读取一个记录。

		bestScores[curListIdx] = node.getKey();

		boolean ret = false;
		Profile.instance.start(ProfileField.UPDATE_CAND.toString());

		for (MidSegment midseg : node.getValue()) {
			if (midseg.getStart() <= query.getEndTime() && midseg.getEndTime() >= query.getStartTime())
				ret |= updateCandState_intern(curListIdx, midseg, 1.0f);
		}
		cand.prune(topk.getMinWorstScore());
		Profile.instance.end(ProfileField.UPDATE_CAND.toString());

		if (!plc.hasNext()) {
			bestScores[curListIdx] = 0;
		}
		return ret;
	}


}
