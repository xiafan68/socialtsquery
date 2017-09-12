package core.lsmi;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import common.MidSegment;
import core.commom.MemoryPostingListIterUtil;
import core.lsmi.SortedListMemTable.SortedListPostinglist;
import core.lsmt.IMemTable.SSTableMeta;
import core.lsmt.IPostingListIterator;
import core.lsmt.ISSTableReader;
import core.lsmt.ISSTableWriter.DirEntry;
import core.lsmt.PostingListMeta;
import core.lsmt.WritableComparable;
import util.Pair;

public class SortedListMemTableReader implements ISSTableReader {
	SortedListMemTable table;

	public SortedListMemTableReader(SortedListMemTable table) {
		this.table = table;
	}

	@Override
	public Iterator<WritableComparable> keySetIter() {
		return table.keySet().iterator();
	}

	@Override
	public IPostingListIterator getPostingListScanner(WritableComparable key) throws IOException {
		return new MemorySortedListIterator(table.get(key), 0, Integer.MAX_VALUE);
	}

	@Override
	public IPostingListIterator getPostingListIter(WritableComparable key, int start, int end) throws IOException {
		return MemoryPostingListIterUtil.getPostingListIter(new MemorySortedListIterator(table.get(key), start, end),
				start, end);
	}

	private static class MemorySortedListIterator implements IPostingListIterator {
		SortedListPostinglist postingList;
		Iterator<MidSegment> iter;
		int start;
		int end;

		MidSegment cur = null;

		public MemorySortedListIterator(SortedListPostinglist postingList, int start, int end) {
			if (postingList != null) {
				this.postingList = postingList;
				this.iter = postingList.iterator();
				this.start = start;
				this.end = end;
			} else {
				postingList = null;
			}
		}

		@Override
		public PostingListMeta getMeta() {
			return postingList.getMeta();
		}

		@Override
		public void open() throws IOException {

		}

		private void advance() {
			while (iter.hasNext()) {
				cur = iter.next();
				if (cur.getStart() <= end && cur.getEndTime() >= start)
					break;
			}
		}

		@Override
		public boolean hasNext() throws IOException {
			if (postingList == null || (cur == null && !iter.hasNext())) {
				return false;
			} else if (cur == null) {
				advance();
			}
			return cur != null;
		}

		@Override
		public Pair<Integer, List<MidSegment>> next() throws IOException {
			if (cur == null)
				advance();
			Pair<Integer, List<MidSegment>> ret = new Pair<Integer, List<MidSegment>>(
					Math.max(cur.getStartCount(), cur.getEndCount()), Arrays.asList(cur));
			cur = null;
			return ret;
		}

		@Override
		public void close() throws IOException {
		}

		@Override
		public void skipTo(WritableComparable key) throws IOException {
			// TODO Auto-generated method stub
		}
	}

	@Override
	public SSTableMeta getMeta() {
		return table.getMeta();
	}

	@Override
	public boolean isInited() {
		return true;
	}

	@Override
	public void init() throws IOException {
	}

	@Override
	public void close() throws IOException {
	}

	@Override
	public DirEntry getDirEntry(WritableComparable key) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}
}
