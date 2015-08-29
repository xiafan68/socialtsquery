package core.lsmi;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import Util.Pair;

import common.MidSegment;

import core.lsmt.IPostingListIterator;
import core.lsmt.ISSTableReader;
import core.lsmt.PostingListMeta;

public class SortedListMemTableReader extends ISSTableReader {
	SortedListMemTable table;

	public SortedListMemTableReader(SortedListMemTable table) {
		super(table.index, table.meta);
		this.table = table;
	}

	@Override
	public Iterator<Integer> keySetIter() {
		return table.keySet().iterator();
	}

	@Override
	public IPostingListIterator getPostingListScanner(int key)
			throws IOException {
		return new SortedListIterator(table.get(key), 0, Integer.MAX_VALUE);
	}

	@Override
	public IPostingListIterator getPostingListIter(int key, int start, int end)
			throws IOException {
		return new SortedListIterator(table.get(key), start, end);
	}

	@Override
	public void close() throws IOException {
	}

	private static class SortedListIterator implements IPostingListIterator {
		Iterator<MidSegment> iter;
		int start;
		int end;

		MidSegment cur = null;

		public SortedListIterator(List<MidSegment> list, int start, int end) {
			this.iter = list.iterator();
			this.start = start;
			this.end = end;
		}

		@Override
		public PostingListMeta getMeta() {

			return null;
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
			if (cur == null && !iter.hasNext()) {
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
					Math.max(cur.getStart(), cur.getEndCount()),
					Arrays.asList(cur));
			cur = null;
			return ret;
		}

		@Override
		public void close() throws IOException {

		}
	}
}
