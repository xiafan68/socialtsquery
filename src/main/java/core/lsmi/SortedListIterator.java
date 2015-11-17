package core.lsmi;

import java.io.IOException;
import java.util.List;

import common.MidSegment;
import core.lsmt.IPostingListIterator;
import core.lsmt.WritableComparableKey;
import util.Pair;
import core.lsmt.PostingListMeta;

public class SortedListIterator implements IPostingListIterator {

	@Override
	public PostingListMeta getMeta() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void open() throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public boolean hasNext() throws IOException {
		// TODO Auto-generated method stub
		return false;
	}

	public MidSegment nextSeg() throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Pair<Integer, List<MidSegment>> next() throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void skipTo(WritableComparableKey key) throws IOException {
		// TODO Auto-generated method stub

	}
}
