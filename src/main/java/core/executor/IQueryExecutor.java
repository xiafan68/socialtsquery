package core.executor;

import java.io.IOException;
import java.util.Iterator;

import segmentation.Interval;
import core.commom.TempKeywordQuery;

/**
 * 定义查询算法的接口,接受一个temporal keyword query，返回topk最受欢迎的items，当前类主要是定义了threhold
 * algorithm所需要的接口
 * 
 * @author xiafan
 * 
 */
public abstract class IQueryExecutor {
	protected int maxLifeTime;

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

	public void setMaxLifeTime(int maxLife) {
		this.maxLifeTime = maxLife;
	}

	/**
	 * 执行一步读取索引元素的操作
	 * 
	 * @return
	 */
	public abstract boolean advance();

	/**
	 * 判断算法执行是否已经结束
	 * 
	 * @return
	 */
	public abstract boolean isTerminated();
}
