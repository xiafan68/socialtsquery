package core.executor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import core.commom.TempKeywordQuery;
import core.executor.domain.KeyedCandQueue;
import core.executor.domain.KeyedTopKQueue;
import core.executor.domain.MergedMidSeg;
import core.lsmt.IPostingListIterator;
import core.lsmt.LSMTInvertedIndex;
import segmentation.Interval;

/**
 * 定义查询算法的接口,接受一个temporal keyword query，返回topk最受欢迎的items，当前类主要是定义了threhold
 * algorithm所需要的接口
 * 
 * @author xiafan
 * 
 */
public abstract class IQueryExecutor {
	protected int maxLifeTime = 60 * 60 * 24 * 365 * 10;
	LSMTInvertedIndex reader;

	TempKeywordQuery query;

	IPostingListIterator[] cursors;
	float[] bestScores;
	int curListIdx = 0;
	KeyedTopKQueue topk;
	KeyedCandQueue cand;
	Map<Long, MergedMidSeg> map = new HashMap<Long, MergedMidSeg>();
	ExecContext ctx;

	boolean stop = false;

	public IQueryExecutor(LSMTInvertedIndex reader) {
		this.reader = reader;
		maxLifeTime = Integer.MAX_VALUE;
	}

	/**
	 * 给定关键词列表和查询时间窗口
	 * 
	 * @param keywords
	 * @param window
	 * @throws IOException
	 */
	public abstract void query(TempKeywordQuery query) throws IOException;

	public abstract void setupQueryContext(KeyedTopKQueue topk, Map<Long, MergedMidSeg> map) throws IOException;

	/**
	 * 返回查询相应的结果
	 * 
	 * @return
	 * @throws IOException
	 */
	public Iterator<Interval> getAnswer() throws IOException {
		setupQueryContext(null, new HashMap<Long, MergedMidSeg>());

		while (!isTerminated())
			advance();

		List<Interval> ret = new ArrayList<Interval>(topk.size());
		for (int i = 0; i < topk.size(); i++) {
			ret.add(null);
		}
		Iterator<MergedMidSeg> iter = topk.iterator();
		int i = topk.size() - 1;
		while (iter.hasNext()) {
			MergedMidSeg cur = iter.next();
			ret.set(i--, new Interval(cur.getMid(), cur.getStartTime(), cur.getEndTime(), cur.getWorstscore()));
		}
		return ret.iterator();
	}

	/**
	 * 设置当前分区内元素的最大生命周期
	 * 
	 * @param maxLife
	 */
	public void setMaxLifeTime(int maxLife) {
		this.maxLifeTime = maxLife;
	}

	/**
	 * 执行一步读取索引元素的操作
	 * 
	 * @return
	 * @throws IOException
	 */
	public abstract boolean advance() throws IOException;

	/**
	 * 判断算法执行是否已经结束
	 * 
	 * @return
	 * @throws IOException
	 */
	public abstract boolean isTerminated() throws IOException;
}
