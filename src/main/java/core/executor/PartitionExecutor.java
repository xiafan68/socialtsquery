package core.executor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import segmentation.Interval;
import Util.MyMath;
import Util.Profile;
import core.executor.domain.ISegQueue;
import core.executor.domain.MergedMidSeg;
import core.executor.domain.SortBestscore;
import core.executor.domain.SortWorstscore;
import core.index.IndexReader;
import core.index.MidSegment;
import core.index.PartitionMeta;
import core.index.PostingListCursor;
import core.index.TempKeywordQuery;

/**
 * 实现一个baseline算法,基于某个partition的索引执行查询
 * 
 * @author dingcheng
 * 
 */
public class PartitionExecutor extends IQueryExecutor {
	IndexReader reader;

	TempKeywordQuery query;

	PostingListCursor[] cursors;
	float[] bestScores;
	int curListIdx = 0;
	ISegQueue topk;
	ISegQueue cand;
	Map<Long, MergedMidSeg> map = new HashMap<Long, MergedMidSeg>();
	ExecContext ctx;

	boolean stop = false;

	public PartitionExecutor(IndexReader reader) {
		this.reader = reader;
	}

	PartitionMeta meta;

	/**
	 * 
	 * @param topk
	 *            用于存放当前topk元素的队列
	 * @param_lifetime 当前BaseExecutor只负责处理所有生命周期不大于lifetime的元素
	 * @throws java.io.IOException
	 */
	public void setupQueryContext(ISegQueue topk, Map<Long, MergedMidSeg> map)
			throws IOException {
		this.map = map;
		if (topk != null)
			this.topk = topk;
		else
			this.topk = ISegQueue.create(new SortWorstscore(), true);
		cand = ISegQueue.create(new SortBestscore(), false);

		ctx = new ExecContext(query);
		int part = MyMath.getCeil(maxLifeTime);
		meta = new PartitionMeta(part);
		int minLt = (int) Math.pow(2, part - 1);
		ctx.setLifeTimeBound(new int[] { minLt, minLt << 2 - 1 });
		bestScores = new float[query.keywords.length];
		Arrays.fill(bestScores, 0);

		ctx.setBestScores(bestScores);

		loadCursors();
	}

	private PostingListCursor getCursor(String keyword, Interval window)
			throws IOException {
		return reader.cursor(keyword, window, meta);
	}

	private void loadCursors() throws IOException {
		String[] keywords = query.keywords;
		cursors = new PostingListCursor[keywords.length];
		/* 为每个词创建索引读取对象 */
		for (int i = 0; i < keywords.length; i++) {
			if (cursors[i] != null) {
				cursors[i].close();
			}
			cursors[i] = getCursor(keywords[i], query.queryInv);
		}
	}

	@Override
	public void query(TempKeywordQuery query) throws IOException {
		this.query = query;
		// 通过keyword和window去索引里查询
	}

	@Override
	public boolean isTerminated() {
		if (stop) {
			return true;
		} else {
			float sum = 0;
			for (float bestScore : bestScores) {
				sum += bestScore;
			}
			sum *= Math.min(maxLifeTime,
					query.getEndTime() - query.getStartTime());
			boolean ret = true;
			// 当前partition不可能有cand能够进入topk
			if (cand.getMaxBestScore() < topk.getMinWorstScore()
					&& sum < topk.getMinWorstScore()
					&& topk.size() >= ctx.getQuery().k) {
				ret = true;
			} else {
				// 所有的倒排表都已经遍历过了
				for (PostingListCursor cursor : cursors) {
					if (cursor != null && cursor.hasNext()) {
						ret = false;
						break;
					}
				}
			}
			stop = ret;

			if (stop) {
				Profile.instance.updateCounter(Profile.TOPK, topk.size());
				Profile.instance.updateCounter(Profile.CAND, cand.size());
			}

			return ret;
		}
	}

	private PostingListCursor nextCursor() {
		PostingListCursor ret = null;
		for (int i = 0; i < cursors.length; i++) {
			int preListIdx = curListIdx;
			curListIdx = (++curListIdx) % cursors.length;

			ret = cursors[preListIdx];
			if (ret == null) {
				continue;
			} else if (!ret.hasNext()) {
				cursors[preListIdx] = null;
				ret = null;
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
	private boolean updateCandState(int idx, MidSegment midseg, float iWeight) {
		MergedMidSeg preSeg = null;
		MergedMidSeg newSeg = null;
		Long mid = midseg.mid;
		/* update the boundary, put it in the QueCand */
		if (map.containsKey(mid)) {
			preSeg = map.get(mid);
			newSeg = preSeg.addMidSeg(idx, midseg, iWeight);// update the merged
		} else {
			newSeg = new MergedMidSeg(ctx);
			newSeg = newSeg.addMidSeg(idx, midseg, iWeight);
		}
		//update the map with the new mergedseg
		map.put(mid, newSeg);

		boolean ret = true;
		/* update the topk and cands */
		if (topk.contains(preSeg)) {
			topk.update(preSeg, newSeg);
		} else if (topk.size() < query.k
				|| newSeg.getWorstscore() > topk.getMinBestScore()) {
			topk.update(preSeg, newSeg);
			if (topk.size() > query.k)
				topk.poll();// 把bestScore最小的一个移除

			if (preSeg != null)
				cand.remove(preSeg);
		} else if (newSeg.getBestscore() > topk.getMinWorstScore()) {
			cand.update(preSeg, newSeg);
		} else if (preSeg != null) {
			cand.remove(preSeg);
			map.remove(preSeg.getMid());
			ret = false;
		} else {
			map.remove(mid);
			ret = false;
			Profile.instance.updateCounter(Profile.WASTED_REC);
		}
		return ret;
	}

	@Override
	public boolean advance() {
		if (isTerminated()) {
			// 所有倒排记录都读完了。
			return false;
		}

		PostingListCursor plc = nextCursor();
		MidSegment midseg = plc.next();// 读取一个记录。

		int idx = curListIdx - 1;
		if (idx < 0)
			idx = query.keywords.length - 1;
		if (plc.hasNext()) {
			bestScores[idx] = plc.getBestScore();
		} else {
			cursors[idx] = null;
			bestScores[idx] = 0.0f;
		}
		
		boolean ret = true;
		Profile.instance.start(Profile.UPDATE_STATE);
		ret = updateCandState(idx, midseg, 1.0f);
		Profile.instance.end(Profile.UPDATE_STATE);
		return ret;
	}

	@Override
	public Iterator<Interval> getAnswer() throws IOException {
		setupQueryContext(null, new HashMap<Long, MergedMidSeg>());

		while (!isTerminated())
			advance();

		List<Interval> ret = new ArrayList<Interval>();
		Iterator<MergedMidSeg> iter = topk.iterator();
		while (iter.hasNext()) {
			MergedMidSeg cur = iter.next();
			ret.add(new Interval(cur.getMid(), cur.getStartTime(), cur
					.getEndTime(), cur.getWorstscore()));
		}
		return ret.iterator();
	}
}
