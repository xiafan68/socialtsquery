package core.index;

import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import Util.Configuration;

import common.MidSegment;

import core.index.MemTable.SSTableMeta;

/**
 * 
 * @author xiafan
 *
 */
public class LSMOInvertedIndex {
	AtomicBoolean running = new AtomicBoolean(true);
	File dataDir;

	MemTable curTable;
	VersionSet version = new VersionSet();
	int maxVersion;

	FlushService flushService;
	CompactService compactService;
	Configuration conf;

	public LSMOInvertedIndex(Configuration conf) {
		this.conf = conf;
	}

	/**
	 * TODO: 1. 扫描磁盘文件，确定一个版本集合。2. 启动写出服务;3. 启动日志服务，开始日志恢复;4. 启动压缩服务，开始接受更新与查询
	 */
	public void init() {
		flushService = new FlushService(this);
		flushService.start();
	}

	private int getKeywordCode(String keyword) {
		return 0;
	}

	public void insert(List<String> keywords, MidSegment seg) {
		LockManager.instance.versionReadLock();
		try {
			for (String keyword : keywords) {
				int code = getKeywordCode(keyword);
				LockManager.instance.postWriteLock(code);
				try {
					curTable.insert(code, seg);
				} finally {
					LockManager.instance.postWriteUnLock(code);
				}
			}
		} finally {
			LockManager.instance.versionReadLock();
		}
		maySwitchMemtable();
	}

	public void insert(String keywords, MidSegment seg) {

	}

	// TODO check whether we need to switch the memtable
	public void maySwitchMemtable() {
		MemTable tmp = curTable;
		if (tmp.size() > conf.getFlushLimit()) {
			LockManager.instance.versionWriteLock();
			try {
				// check for violation again
				if (tmp == curTable) {
					// open new log
					CommitLog.instance.openNewLog(maxVersion++);
					SSTableMeta meta = new SSTableMeta(maxVersion++, 0);
					tmp = new MemTable(meta);
					version = version.clone();
					version.newMemTable(tmp);
					curTable = tmp;
				}
			} finally {
				LockManager.instance.versionWriteUnLock();
			}
		}
	}

	public void flushTables(Set<Integer> versions, SSTableMeta newMeta) {
		LockManager.instance.versionWriteLock();
		version = version.clone();
		version.flush(versions, newMeta);
		LockManager.instance.versionWriteUnLock();
	}

	public int getCompactNum() {
		return 1;
	}

	public int getFlushNum() {
		// TODO Auto-generated method stub
		return 1;
	}

	public VersionSet getVersion() {
		LockManager.instance.versionReadLock();
		VersionSet ret = version;
		LockManager.instance.versionReadUnLock();
		return ret;
	}

	public int getStep() {
		return conf.getIndexStep();
	}

	public Configuration getConf() {
		return conf;
	}

	/**
	 * record the current memory tree, flushing memory tree and disk trees
	 * @author xiafan
	 *
	 */
	public static class VersionSet {
		public MemTable curTable = null;
		public List<MemTable> flushingTables = new ArrayList<MemTable>();
		public List<SSTableMeta> diskTreeMetas = new ArrayList<SSTableMeta>();

		// public HashMap<OctreeMeta>

		/**
		 * client grantuee thread safe, used when flushing current octree
		 * @param newTree
		 */
		public void newMemTable(MemTable newTable) {
			if (curTable != null) {
				curTable.freeze();
				flushingTables.add(curTable);
			}
			curTable = newTable;
		}

		/**
		 * used when a set of memory octree are flushed to disk as one single unit
		 * @param versions
		 * @param newMeta
		 */
		public void flush(Set<Integer> versions, SSTableMeta newMeta) {
			Iterator<MemTable> iter = flushingTables.iterator();
			while (iter.hasNext()) {
				MemTable table = iter.next();
				if (versions.contains(table.getMeta().version)) {
					iter.remove();
				}
			}
			diskTreeMetas.add(newMeta);
		}

		/**
		 * used when a set of version a compacted
		 * @param versions
		 * @param newMeta
		 */
		public void compact(Set<Integer> versions, SSTableMeta newMeta) {
			Iterator<SSTableMeta> iter = diskTreeMetas.iterator();
			while (iter.hasNext()) {
				SSTableMeta meta = iter.next();
				if (versions.contains(meta.version)) {
					iter.remove();
				}
			}
			diskTreeMetas.add(newMeta);
		}

		@Override
		public VersionSet clone() {
			VersionSet ret = new VersionSet();
			ret.curTable = curTable;
			ret.flushingTables.addAll(flushingTables);
			ret.diskTreeMetas.addAll(diskTreeMetas);
			return ret;
		}
	}
}
