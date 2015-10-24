package core.executor;

import core.lsmt.LSMTInvertedIndex;

public class QueryExecutorFactory {
	public static enum ExecType {
		AND, OR, WEIGHTED
	}

	public static IQueryExecutor createExecutor(LSMTInvertedIndex index, String type) {
		return createExecutor(index, ExecType.valueOf(type));
	}

	public static IQueryExecutor createExecutor(LSMTInvertedIndex index, ExecType type) {
		if (type == ExecType.AND) {
			return new AndQueryExecutor(index);
		} else if (type == ExecType.OR) {
			return new OrQueryExecutor(index);
		} else {
			return new WeightedQueryExecutor(index);
		}
	}
}
