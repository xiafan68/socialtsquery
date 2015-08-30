package core.lsmi;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.TreeMap;

import common.MidSegment;

import core.lsmi.SortedListMemTable.SortedListPostinglist;
import core.lsmt.IMemTable;
import core.lsmt.ISSTableReader;
import core.lsmt.LSMTInvertedIndex;
import core.lsmt.PostingListMeta;

public class SortedListMemTable extends TreeMap<Integer, SortedListPostinglist>
		implements IMemTable<List<MidSegment>> {
	SSTableMeta meta;
	// for the reason of multiple thread
	private volatile boolean frezen = false;
	private volatile int valueCount = 0;
	LSMTInvertedIndex index;

	public SortedListMemTable(LSMTInvertedIndex index, SSTableMeta meta) {
		this.meta = meta;
		this.index = index;
	}

	@Override
	public ISSTableReader getReader() {
		return new SortedListMemTableReader(this);
	}

	@Override
	public SSTableMeta getMeta() {
		return meta;
	}

	@Override
	public void freeze() {
		frezen = true;

	}

	@Override
	public int size() {
		return valueCount;
	}

	@Override
	public void insert(int key, MidSegment seg) {
		if (frezen) {
			throw new RuntimeException("insertion on frezen memtable!!!");
		}

		valueCount++;
		SortedListPostinglist postinglist;
		if (containsKey(key)) {
			postinglist = get(key);
		} else {
			postinglist = new SortedListPostinglist();
			put(key, postinglist);
		}
		postinglist.insert(seg);
	}

	@Override
	public Iterator<Entry<Integer, List<MidSegment>>> iterator() {
		return this.iterator();
	}

	public static class SortedListPostinglist {
		List<MidSegment> list = new ArrayList<MidSegment>();
		PostingListMeta meta = new PostingListMeta();

		public PostingListMeta getMeta() {
			return meta;
		}

		public void insert(MidSegment seg) {
			meta.size++;
			meta.minTime = Math.min(meta.minTime, seg.getStart());
			meta.maxTime = Math.max(meta.maxTime, seg.getEndTime());
			int idx = Collections.binarySearch(list, seg);
			if (idx < 0) {
				idx = Math.abs(idx) - 1;
			}
			list.add(idx, seg);
		}

		public Iterator<MidSegment> iterator() {
			return list.iterator();
		}
	}

}
