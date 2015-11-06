package core.executor;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import segmentation.Interval;
import core.commom.TempKeywordQuery;
import core.executor.domain.CandQueue;
import core.executor.domain.MergedMidSeg;
import core.executor.domain.TopkQueue;
import core.lsmt.IPostingListIterator;
import core.lsmt.LSMTInvertedIndex;

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
	TopkQueue topk;
	CandQueue cand;
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

	/**
	 * 返回查询相应的结果
	 * 
	 * @return
	 * @throws IOException
	 */
	public abstract Iterator<Interval> getAnswer() throws IOException;

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
