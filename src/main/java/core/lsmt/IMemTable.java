package core.lsmt;

import java.io.Serializable;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicBoolean;

import common.MidSegment;

/**
 * 常驻内存中的memtable的抽象，这里对于postinglist的类型支持抽象，从而能够兼容各种实现
 * 
 * @author xiafan
 *
 * @param <VType>
 *            postinglist的实现类
 */
public interface IMemTable<VType> {

	/**
	 * @return the reader
	 */
	public ISSTableReader getReader();

	/**
	 * @return the meta
	 */
	public SSTableMeta getMeta();

	public void freeze();

	public int size();

	public void insert(int key, MidSegment seg);

	public Iterator<Entry<Integer, VType>> iterator();

	public static class SSTableMeta implements Serializable, Comparable<SSTableMeta> {
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

		/*
		 * (non-Javadoc)
		 * 
		 * @see java.lang.Object#toString()
		 */
		@Override
		public String toString() {
			return "SSTableMeta [version=" + version + ", level=" + level + ", persisted=" + persisted + ", markAsDel="
					+ markAsDel + "]";
		}
	}

}
