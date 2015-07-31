package core.index;

import java.io.Serializable;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;

import common.MidSegment;
import core.index.octree.MemoryOctree;
import core.index.octree.MemoryOctree.OctreeMeta;

public class MemTable extends TreeMap<Integer, MemoryOctree> {
	private static final long serialVersionUID = 1L;
	private SSTableMeta meta;
	// for the reason of multiple thread
	private volatile boolean frezen = false;
	private volatile int valueCount = 0;
	private LSMOInvertedIndex index;
	private final MemorySSTableReader reader;

	public MemTable(LSMOInvertedIndex index, SSTableMeta meta) {
		this.meta = meta;
		this.index = index;
		this.reader = new MemorySSTableReader(index, this, meta);
	}

	/**
	 * @return the reader
	 */
	public MemorySSTableReader getReader() {
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

	public static class SSTableMeta implements Serializable,
			Comparable<SSTableMeta> {
		public int version;
		public int level = 0;
		transient boolean persisted = true;
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

		@Override
		public int compareTo(SSTableMeta o) {
			int ret = Integer.compare(version, o.version);
			if (ret == 0)
				ret = Integer.compare(level, o.level);
			return ret;
		}

		@Override
		public int hashCode() {
			return version + level;
		}

		@Override
		public boolean equals(Object other) {
			if (other instanceof SSTableMeta) {
				SSTableMeta oMeta = (SSTableMeta) other;
				return version == oMeta.version && level == oMeta.level;
			}
			return false;
		}

		public SSTableMeta clone() {
			SSTableMeta meta = new SSTableMeta(version, level);
			meta.markAsDel = markAsDel;
			meta.persisted = persisted;
			return meta;
		}

		/* (non-Javadoc)
		 * @see java.lang.Object#toString()
		 */
		@Override
		public String toString() {
			return "SSTableMeta [version=" + version + ", level=" + level
					+ ", persisted=" + persisted + ", markAsDel=" + markAsDel
					+ "]";
		}
	}
}
