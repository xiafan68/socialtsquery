package core.executor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import segmentation.Interval;
import core.executor.domain.ISegQueue;
import core.executor.domain.MergedMidSeg;
import core.executor.domain.SortWorstscore;
import core.index.IndexReader;
import core.index.PartitionMeta;
import core.index.TempKeywordQuery;

/**
 * <p>
 * 当前算法假设底层的数据是按照元素的生命周期进行分割的，它重用PartitionExecutor在每个partition的数据上执行threshold
 * algorithm
 * </p>
 * 
 * @author xiafan
 * 
 */
public class MultiPartitionExecutor extends IQueryExecutor {
	private static final int MAX_LIFETIME = 31;
	private IndexReader reader;

	List<PartitionExecutor> executors = new ArrayList<PartitionExecutor>();
	ISegQueue topk;
	TempKeywordQuery query;

	public MultiPartitionExecutor(IndexReader reader) {
		this.reader = reader;
	}

	@Override
	public boolean advance() {
		Iterator<PartitionExecutor> iter = executors.iterator();
		while (iter.hasNext()) {
			PartitionExecutor cur = iter.next();
			for (int i = 0; i < 1 && cur.advance(); i++)
				;
			if (cur.isTerminated()) {
				iter.remove();
			}
		}
		return true;
	}

	@Override
	public void query(TempKeywordQuery query) throws IOException {
		this.query = query;
		topk = ISegQueue.create(new SortWorstscore(), true);
		Map<Long, MergedMidSeg> map = new HashMap<Long, MergedMidSeg>();
		for (PartitionMeta meta : reader.getPartitions()) {
			PartitionExecutor exe = new PartitionExecutor(reader);
			exe.query(query);
			exe.setMaxLifeTime(meta.getLifetimeBound());
			exe.setupQueryContext(topk, map);
			executors.add(exe);
		}

		while (!executors.isEmpty()) {
			advance();
		}
	}

	@Override
	public Iterator<Interval> getAnswer() throws IOException {
		List<Interval> ret = new ArrayList<Interval>();
		Iterator<MergedMidSeg> iter = topk.iterator();
		while (iter.hasNext()) {
			MergedMidSeg cur = iter.next();
			ret.add(new Interval(cur.getMid(), query.getStartTime(), query
					.getEndTime(), cur.getWorstscore()));
		}
		return ret.iterator();
	}

	@Override
	public boolean isTerminated() {
		return executors.isEmpty();
	}

}
