package core.lsmo;

import java.util.Iterator;
import java.util.Map.Entry;

import common.MidSegment;
import core.lsmo.octree.MemoryOctree;
import core.lsmt.IMemTable.SSTableMeta;
import core.lsmt.ISSTableReader;
import core.lsmt.InvertedMemtable;
import core.lsmt.LSMTInvertedIndex;
import core.lsmt.PostingListMeta;
import core.lsmt.WritableComparableKey;

public class OctreeMemTable extends InvertedMemtable<MemoryOctree> {
	private static final long serialVersionUID = 1L;
	private SSTableMeta meta;
	// for the reason of multiple thread
	private volatile boolean frezen = false;
	private volatile int valueCount = 0;
	private LSMTInvertedIndex index;
	private final MemorySSTableReader reader;

	public OctreeMemTable(LSMTInvertedIndex index, SSTableMeta meta) {
		this.meta = meta;
		this.index = index;
		this.reader = new MemorySSTableReader(this, meta);
	}

	/**
	 * @return the reader
	 */
	public ISSTableReader getReader() {
		return reader;
	}

	/**
	 * @return the meta
	 */
	public SSTableMeta getMeta() {
		return meta;
	}

	public void freeze() {
		frezen = true;
	}

	public int size() {
		return valueCount;
	}

	public void insert(WritableComparableKey key, MidSegment seg) {
		if (frezen) {
			throw new RuntimeException("insertion on frezen memtable!!!");
		}

		valueCount++;
		MemoryOctree postinglist;
		if (containsKey(key)) {
			postinglist = get(key);
		} else {
			postinglist = new MemoryOctree(new PostingListMeta());
			put(key, postinglist);
		}
		postinglist.insert(seg.getPoint(), seg);
	}

	public Iterator<Entry<WritableComparableKey, MemoryOctree>> iterator() {
		return super.entrySet().iterator();
	}

}
