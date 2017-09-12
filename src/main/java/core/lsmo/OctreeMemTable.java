package core.lsmo;

import common.MidSegment;
import core.lsmo.octree.MemoryOctree;
import core.lsmt.ISSTableReader;
import core.lsmt.InvertedMemtable;
import core.lsmt.LSMTInvertedIndex;
import core.lsmt.PostingListMeta;
import core.lsmt.WritableComparable;

public class OctreeMemTable extends InvertedMemtable<MemoryOctree> {
	private static final long serialVersionUID = 1L;

	private final MemorySSTableReader reader;
	protected LSMTInvertedIndex index;
	private long createAt = System.currentTimeMillis();

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

	public void insert(WritableComparable key, MidSegment seg) {
		if (frezen) {
			throw new RuntimeException("insertion on frezen memtable!!!");
		}

		valueCount++;
		MemoryOctree postinglist;
		if (containsKey(key)) {
			postinglist = (MemoryOctree) get(key);
		} else {
			postinglist = new MemoryOctree(new PostingListMeta(), index.getConf().getOctantSizeLimit());
			put(key, postinglist);
		}
		postinglist.insert(seg);
	}

	@Override
	public long createAt() {
		return createAt;
	}
}
