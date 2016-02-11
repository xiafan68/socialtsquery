package expr.runner;

import java.util.List;

import common.MidSegment;
import expr.WorkLoadGen.WorkLoad;
import searchapi.TKeywordsQuery;
import util.Pair;

public interface ITestRunner {
	public void execWorkLoad(WorkLoad load);

	public void execQuery(TKeywordsQuery query);

	public void execUpdate(Pair<List<String>, MidSegment> update);
}
