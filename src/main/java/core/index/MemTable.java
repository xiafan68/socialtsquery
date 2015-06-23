package core.index;

import java.io.Serializable;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import Util.PeekIterDecorate;

import common.MidSegment;

import core.index.octree.MemoryOctree;
import core.index.octree.MemoryOctree.OctreeMeta;

public class MemTable extends TreeMap<Integer, MemoryOctree> {
	private SSTableMeta meta;
	private boolean frezen = false;
	private AtomicInteger valueCount = new AtomicInteger(0);

	public MemTable(SSTableMeta meta) {
		this.meta = meta;
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
		return valueCount.get();
	}

	public void insert(int key, MidSegment seg) {
		valueCount.incrementAndGet();
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
		return PeekIterDecorate.decorate(super.entrySet().iterator());
	}

	public static class SSTableMeta implements Serializable {
		public int version;
		public int level = 0;
		// number of items
		// runtime state
		// reference for disk file
		public transient AtomicInteger ref = new AtomicInteger(0);
		// whether the disk file has been marked as deleted after compaction
		public transient AtomicBoolean markAsDel = new AtomicBoolean(false);

		public SSTableMeta() {

		}

		public SSTableMeta(int version) {
			this.version = version;
		}

		public SSTableMeta(int version, int level) {
			this.version = version;
			this.level = level;
		}
	}
}
