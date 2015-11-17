package core.executor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import core.commom.TempKeywordQuery;
import core.executor.domain.KeyedTopKQueue;
import core.executor.domain.MergedMidSeg;
import core.lsmt.LSMTInvertedIndex;
import core.lsmt.PartitionMeta;
import segmentation.Interval;

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
	private LSMTInvertedIndex reader;

	List<WeightedQueryExecutor> executors = new ArrayList<WeightedQueryExecutor>();
	KeyedTopKQueue topk;
	TempKeywordQuery query;

	public MultiPartitionExecutor(LSMTInvertedIndex reader) {
		super(reader);
		this.reader = reader;
	}

	@Override
	public boolean advance() throws IOException {
		Iterator<WeightedQueryExecutor> iter = executors.iterator();
		while (iter.hasNext()) {
			WeightedQueryExecutor cur = iter.next();
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
		topk = new KeyedTopKQueue();
		Map<Long, MergedMidSeg> map = new HashMap<Long, MergedMidSeg>();
		for (int i = 0; i < reader.getPartitions().size(); i++) {
			PartitionMeta meta = (PartitionMeta) reader.getPartitions().get(i);
			WeightedQueryExecutor exe = new WeightedQueryExecutor(reader);
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
			ret.add(new Interval(cur.getMid(), query.getStartTime(), query.getEndTime(), cur.getWorstscore()));
		}
		return ret.iterator();
	}

	@Override
	public boolean isTerminated() {
		return executors.isEmpty();
	}

}
