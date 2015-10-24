package core.executor;

import java.io.IOException;
import java.util.Iterator;

import core.commom.TempKeywordQuery;

public class AndQueryExecutor extends IQueryExecutor {

	@Override
	public void query(TempKeywordQuery query) throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public boolean advance() throws IOException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean isTerminated() throws IOException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public Iterator<Interval> getAnswer() throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

}
