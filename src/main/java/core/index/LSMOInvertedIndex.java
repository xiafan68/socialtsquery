package core.index;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;

import segmentation.Interval;
import shingle.TextShingle;
import Util.Configuration;

import common.MidSegment;

import core.index.MemTable.SSTableMeta;
import core.index.octree.IOctreeIterator;

/**
 * 
 * @author xiafan
 *
 */
public class LSMOInvertedIndex {
	private static final Logger logger = Logger
			.getLogger(LSMOInvertedIndex.class);

	AtomicBoolean running = new AtomicBoolean(true);
	File dataDir;
	boolean bootstrap = true;

	MemTable curTable;
	VersionSet versionSet = new VersionSet();
	int maxVersion = 0;

	FlushService flushService;
	CompactService compactService;
	Configuration conf;
	TextShingle shingle = new TextShingle(null);

	public LSMOInvertedIndex(Configuration conf) {
		this.conf = conf;
	}

	/**
	 * TODO: 1. 扫描磁盘文件，确定一个版本集合。2. 启动写出服务;3. 启动日志服务，开始日志恢复;4. 启动压缩服务，开始接受更新与查询
	 * @throws IOException 
	 */
	public void init() throws IOException {
		bootstrap = true;
		flushService = new FlushService(this);
		flushService.start();
		LockManager.INSTANCE.setBootStrap();
		setupVersionSet();
		CommitLog.INSTANCE.init(conf);
		CommitLog.INSTANCE.recover(this);
		CommitLog.INSTANCE.openNewLog(curTable.getMeta().version);
		compactService = new CompactService(this);
		compactService.start();
		LockManager.INSTANCE.setBootStrap();
		bootstrap = false;
	}

	private static final Pattern regex = Pattern.compile("%d_%d.meta");

	private void setupVersionSet() {
		File[] indexFiles = conf.getIndexDir().listFiles(new FilenameFilter() {
			@Override
			public boolean accept(File dir, String name) {
				Matcher match = regex.matcher(name);
				return match.matches();
			}
		});
		Arrays.sort(indexFiles, new Comparator<File>() {
			@Override
			public int compare(File o1, File o2) {
				int[] v1 = parseVersion(o1);
				int[] v2 = parseVersion(o2);
				int ret = Integer.compare(v1[0], v2[0]);
				if (ret == 0) {
					ret = Integer.compare(v1[1], v2[1]);
				}
				return ret;
			}
		});
		maxVersion = 0;
		for (File file : indexFiles) {
			int[] version = parseVersion(file);
			SSTableMeta meta = new SSTableMeta(version[0], version[1]);
			if (validateDataFile(meta)) {
				versionSet.diskTreeMetas.add(meta);
				maxVersion = Math.max(maxVersion, meta.version + 1);
			}
		}
		// TODO may need to check if compact fails during moving file
		SSTableMeta meta = new SSTableMeta(maxVersion++, 0);
		curTable = new MemTable(meta);
		versionSet.newMemTable(curTable);
	}

	private boolean validateDataFile(SSTableMeta meta) {
		File file = SSTableWriter.dataFile(conf.getIndexDir(), meta);
		if (!file.exists()) {
			return false;
		}
		file = SSTableWriter.idxFile(conf.getIndexDir(), meta);
		if (!file.exists()) {
			return false;
		}
		file = SSTableWriter.dirMetaFile(conf.getIndexDir(), meta);
		if (!file.exists()) {
			return false;
		}
		return true;
	}

	private static int[] parseVersion(File file) {
		int[] ret = new int[2];
		String[] field = file.getName().split("_");
		ret[0] = Integer.parseInt(field[0]);
		ret[1] = Integer.parseInt(field[1].substring(0, field[1].indexOf('.')));
		return ret;
	}

	private int getKeywordCode(String keyword) {
		return 0;
	}

	public void insert(List<String> keywords, MidSegment seg)
			throws IOException {
		LockManager.INSTANCE.versionReadLock();
		try {
			for (String keyword : keywords) {
				if (!bootstrap)
					CommitLog.INSTANCE.write(keyword, seg);
				int code = getKeywordCode(keyword);
				LockManager.INSTANCE.postWriteLock(code);
				try {
					curTable.insert(code, seg);
				} finally {
					LockManager.INSTANCE.postWriteUnLock(code);
				}
			}
		} finally {
			LockManager.INSTANCE.versionReadUnLock();
		}
		maySwitchMemtable();
	}

	public void insert(String keywords, MidSegment seg) throws IOException {
		insert(shingle.shingling(keywords), seg);
	}

	// TODO check whether we need to switch the memtable
	public void maySwitchMemtable() {
		MemTable tmp = curTable;
		if (tmp.size() > conf.getFlushLimit()) {
			LockManager.INSTANCE.versionWriteLock();
			try {
				// check for violation again
				if (tmp == curTable) {
					// open new log
					CommitLog.INSTANCE.openNewLog(maxVersion++);
					SSTableMeta meta = new SSTableMeta(maxVersion++, 0);
					tmp = new MemTable(meta);
					versionSet = versionSet.clone();
					versionSet.newMemTable(tmp);
					curTable = tmp;
				}
			} finally {
				LockManager.INSTANCE.versionWriteUnLock();
			}
		}
	}

	/**
	 * invoked after a set of versions are flushed. It will update 
	 * the version set and delete corresponding log files
	 * @param versions
	 * @param newMeta
	 */
	public void flushTables(Set<Integer> versions, SSTableMeta newMeta) {
		logger.info("flushed tables for versions " + versions);
		LockManager.INSTANCE.versionWriteLock();
		versionSet = versionSet.clone();
		versionSet.flush(versions, newMeta);

		// mark redo logs as deleted
		for (int version : versions)
			CommitLog.INSTANCE.deleteLog(version);
		LockManager.INSTANCE.versionWriteUnLock();
	}

	public void compactTables(Set<Integer> versions, SSTableMeta newMeta) {
		logger.info("compacted versions for " + versions);
		LockManager.INSTANCE.versionWriteLock();
		versionSet = versionSet.clone();
		versionSet.compact(versions, newMeta);
		LockManager.INSTANCE.versionWriteUnLock();
	}

	public int getCompactNum() {
		return 1;
	}

	public int getFlushNum() {
		// TODO Auto-generated method stub
		return 1;
	}

	public VersionSet getVersion() {
		LockManager.INSTANCE.versionReadLock();
		VersionSet ret = versionSet;
		LockManager.INSTANCE.versionReadUnLock();
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

	public void close() {
		LockManager.INSTANCE.versionWriteLock();
		try {
			CommitLog.INSTANCE.shutdown();
		} finally {
			LockManager.INSTANCE.versionWriteUnLock();
		}
		LockManager.INSTANCE.shutdown();
	}

	public SSTableReader getSSTableReader(SSTableMeta meta) {
		// TODO Auto-generated method stub
		return null;
	}

	public IOctreeIterator getTemporalIterator(String keyword, Interval window) {
		// TODO Auto-generated method stub
		return null;
	}
}
