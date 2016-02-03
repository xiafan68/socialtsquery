package expr.runner;

import java.util.List;

import common.MidSegment;
import expr.WorkLoadGen.TKQuery;
import expr.WorkLoadGen.WorkLoad;
import util.Pair;

public interface ITestRunner {
	public void execWorkLoad(WorkLoad load);

	public void execQuery(TKQuery query);

	public void execUpdate(Pair<List<String>, MidSegment> update);
}
