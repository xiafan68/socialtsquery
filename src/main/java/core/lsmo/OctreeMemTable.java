package core.lsmo;

import java.util.Iterator;
import java.util.Map.Entry;
import java.util.TreeMap;

import common.MidSegment;
import core.lsmo.octree.MemoryOctree;
import core.lsmo.octree.MemoryOctree.OctreeMeta;
import core.lsmt.IMemTable;
import core.lsmt.ISSTableReader;
import core.lsmt.LSMOInvertedIndex;

public class OctreeMemTable extends TreeMap<Integer, MemoryOctree>implements IMemTable<MemoryOctree> {
	private static final long serialVersionUID = 1L;
	private SSTableMeta meta;
	// for the reason of multiple thread
	private volatile boolean frezen = false;
	private volatile int valueCount = 0;
	private LSMOInvertedIndex index;
	private final MemorySSTableReader reader;

	public OctreeMemTable(LSMOInvertedIndex index, SSTableMeta meta) {
		this.meta = meta;
		this.index = index;
		this.reader = new MemorySSTableReader(index, this, meta);
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

	public void insert(int key, MidSegment seg) {
		if (frezen) {
			throw new RuntimeException("insertion on frezen memtable!!!");
		}

		valueCount++;
		MemoryOctree postinglist;
		if (containsKey(key)) {
			postinglist = get(key);
		} else {
			postinglist = new MemoryOctree(new OctreeMeta());
			put(key, postinglist);
		}
		postinglist.insert(seg.getPoint(), seg);
	}

	public Iterator<Entry<Integer, MemoryOctree>> iterator() {
		return super.entrySet().iterator();
	}

}
