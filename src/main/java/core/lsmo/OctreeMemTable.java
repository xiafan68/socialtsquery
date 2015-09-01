package core.lsmo;

import java.util.Iterator;

import common.MidSegment;

import core.lsmo.octree.MemoryOctree;
import core.lsmt.ISSTableReader;
import core.lsmt.InvertedMemtable;
import core.lsmt.LSMTInvertedIndex;
import core.lsmt.PostingListMeta;
import core.lsmt.WritableComparableKey;

public class OctreeMemTable extends InvertedMemtable<MemoryOctree> {
	private static final long serialVersionUID = 1L;

	private final MemorySSTableReader reader;
	protected LSMTInvertedIndex index;

	public OctreeMemTable(LSMTInvertedIndex index, SSTableMeta meta) {
		super(meta);
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
