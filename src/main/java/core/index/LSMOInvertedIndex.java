package core.index;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;

import Util.Configuration;
import common.MidSegment;
import core.index.MemTable.SSTableMeta;
import core.index.octree.IOctreeIterator;
import core.index.octree.OctreeMergeView;
import shingle.TextShingle;

/**
 * 
 * @author xiafan
 *
 */
public class LSMOInvertedIndex {
	private static final Logger logger = Logger.getLogger(LSMOInvertedIndex.class);

	// AtomicBoolean running = new AtomicBoolean(true);
	File dataDir;
	boolean bootstrap = true;
	volatile boolean stop = false;
	MemTable curTable;
	volatile VersionSet versionSet = new VersionSet();
	int maxVersion = 0;

	FlushService flushService;
	CompactService compactService;
	Configuration conf;
	TextShingle shingle = new TextShingle(null);
	DataOutputStream keyWriter;

	public LSMOInvertedIndex(Configuration conf) {
		this.conf = conf;
	}

	/**
	 * TODO: 1. 扫描磁盘文件，确定一个版本集合。2. 启动写出服务;3. 启动日志服务，开始日志恢复;4. 启动压缩服务，开始接受更新与查询
	 * 
	 * @throws IOException
	 */
	public void init() throws IOException {
		bootstrap = true;
		flushService = new FlushService(this);
		flushService.start();
		LockManager.INSTANCE.setBootStrap();

		File keyFile = new File(conf.getIndexDir(), "keyfile.meta");
		DataInputStream dis = null;
		try {
			dis = new DataInputStream(new FileInputStream(keyFile));
			while (dis.available() > 0) {
				keyCode.put(dis.readUTF(), dis.readInt());
			}
		} catch (Exception exception) {
		} finally {
			if (dis != null)
				dis.close();
		}
		keyWriter = new DataOutputStream(new FileOutputStream(keyFile, true));

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
		// TODO 这一段还没有搞定
		List<File> exactVersion = new ArrayList<File>();
		for (File file : indexFiles) {
			int[] v1 = parseVersion(file);
			while (!exactVersion.isEmpty()) {
				File last = exactVersion.get(exactVersion.size() - 1);
				int[] v2 = parseVersion(last);
				if (v1[0] >= v2[0] && v1[1] > v2[1]) {
					// TODO last file should be removed
					exactVersion.remove(exactVersion.size() - 1);
				} else {
					break;
				}
			}
			exactVersion.add(file);
		}

		maxVersion = 0;
		for (File file : exactVersion) {
			int[] version = parseVersion(file);
			SSTableMeta meta = new SSTableMeta(version[0], version[1]);
			if (validateDataFile(meta)) {
				versionSet.diskTreeMetas.add(meta);
				maxVersion = Math.max(maxVersion, meta.version + 1);
			}
		}
		SSTableMeta meta = new SSTableMeta(maxVersion++, 0);
		curTable = new MemTable(this, meta);
		versionSet.newMemTable(curTable);
	}

	/**
	 * 验证是否所有的索引文件都存在
	 * 
	 * @param meta
	 * @return
	 */
	private boolean validateDataFile(SSTableMeta meta) {
		File dFile = SSTableWriter.dataFile(conf.getIndexDir(), meta);
		File idxFile = SSTableWriter.idxFile(conf.getIndexDir(), meta);
		File dirFile = SSTableWriter.dirMetaFile(conf.getIndexDir(), meta);
		if (!dFile.exists() || !idxFile.exists() || !dirFile.exists()) {
			logger.warn("index file of version " + meta + " is not consistent");
			return false;
		}
		return true;
	}

	public static int[] parseVersion(File file) {
		int[] ret = new int[2];
		String[] field = file.getName().split("_");
		ret[0] = Integer.parseInt(field[0]);
		ret[1] = Integer.parseInt(field[1].substring(0, field[1].indexOf('.')));
		return ret;
	}

	Map<String, Integer> keyCode = new HashMap<String, Integer>();

	int getKeywordCode(String keyword) throws IOException {
		if (!keyCode.containsKey(keyword)) {
			keyCode.put(keyword, keyCode.size());
			keyWriter.writeUTF(keyword);
			keyWriter.writeInt(keyCode.get(keyword));
		}
		return keyCode.get(keyword);
	}

	public void insert(List<String> keywords, MidSegment seg) throws IOException {
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
					CommitLog.INSTANCE.openNewLog(maxVersion);
					SSTableMeta meta = new SSTableMeta(maxVersion++, 0);
					tmp = new MemTable(this, meta);
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
	 * invoked after a set of versions are flushed. It will update the version
	 * set and delete corresponding log files
	 * 
	 * @param versions
	 * @param newMeta
	 */
	public void flushTables(Set<SSTableMeta> versions, SSTableMeta newMeta) {
		logger.info("flushed tables for versions " + versions);
		LockManager.INSTANCE.versionWriteLock();
		versionSet = versionSet.clone();
		versionSet.flush(versions, newMeta);

		// mark redo logs as deleted
		for (SSTableMeta version : versions)
			CommitLog.INSTANCE.deleteLog(version.version);
		LockManager.INSTANCE.versionWriteUnLock();
	}

	public void compactTables(Set<SSTableMeta> versions, SSTableMeta newMeta) {
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
		// LockManager.INSTANCE.versionReadLock();
		// VersionSet ret = versionSet;
		// LockManager.INSTANCE.versionReadUnLock();
		return versionSet;
	}

	public Map<String, IOctreeIterator> getPostingListIter(List<String> keywords, int start, int end)
			throws IOException {
		Map<String, IOctreeIterator> ret = new HashMap<String, IOctreeIterator>();
		for (String keyword : keywords) {
			OctreeMergeView view = new OctreeMergeView();
			int key = getKeywordCode(keyword);
			// add iter for current memtable
			view.addIterator(versionSet.curTable.getReader().getPostingListIter(key, start, end));
			// add iter for flushing memtable
			for (MemTable table : versionSet.flushingTables) {
				view.addIterator(table.getReader().getPostingListIter(key, start, end));
			}
			for (SSTableMeta meta : versionSet.diskTreeMetas) {
				ISSTableReader reader = getSSTableReader(versionSet, meta);
				view.addIterator(reader.getPostingListIter(key, start, end));
			}
			ret.put(keyword, view);
		}
		return ret;
	}

	public int getStep() {
		return conf.getIndexStep();
	}

	public Configuration getConf() {
		return conf;
	}

	// 所有versionset使用的SSTableMeta除了作为readers的key之外，不能有其它地方使用
	ConcurrentMap<SSTableMetaKey, ISSTableReader> readers = new ConcurrentHashMap<SSTableMetaKey, ISSTableReader>();
	ReferenceQueue<SSTableMeta> delMetaQueue = new ReferenceQueue<SSTableMeta>();

	private static class SSTableMetaKey extends WeakReference<SSTableMeta> {
		int version;
		int level;

		public SSTableMetaKey(SSTableMeta referent, ReferenceQueue<SSTableMeta> queue) {
			super(referent, queue);
			version = referent.version;
			level = referent.level;
		}

		@Override
		public boolean equals(Object other) {
			if (!(other instanceof SSTableMetaKey)) {
				return false;
			}
			SSTableMetaKey key = (SSTableMetaKey) other;
			return version == key.version && level == key.level;
		}

		@Override
		public int hashCode() {
			return version + level;
		}
	}

	/**
	 * record the current memory tree, flushing memory tree and disk trees
	 * 
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
		 * 
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
		 * used when a set of memory octree are flushed to disk as one single
		 * unit
		 * 
		 * @param versions
		 * @param newMeta
		 */
		public void flush(Set<SSTableMeta> versions, SSTableMeta newMeta) {
			Iterator<MemTable> iter = flushingTables.iterator();
			while (iter.hasNext()) {
				MemTable table = iter.next();
				if (versions.contains(table.getMeta())) {
					iter.remove();
				}
			}
			diskTreeMetas.add(newMeta);
		}

		/**
		 * used when a set of version a compacted
		 * 
		 * @param versions
		 * @param newMeta
		 */
		public void compact(Set<SSTableMeta> versions, SSTableMeta newMeta) {
			Iterator<SSTableMeta> iter = diskTreeMetas.iterator();
			while (iter.hasNext()) {
				SSTableMeta meta = iter.next();
				if (versions.contains(meta)) {
					iter.remove();
					meta.markAsDel.set(true);
				}
			}
			diskTreeMetas.add(newMeta);
		}

		/**
		 * the clone operation may be expensive, but I believe it is not a quite
		 * frequent operation
		 */
		@Override
		public VersionSet clone() {
			VersionSet ret = new VersionSet();
			ret.curTable = curTable;
			ret.flushingTables.addAll(flushingTables);
			ret.diskTreeMetas.addAll(diskTreeMetas);
			return ret;
		}
	}

	public void close() throws IOException {
		keyWriter.close();
		stop = true;
		while (true) {
			try {
				flushService.join();
				break;
			} catch (InterruptedException e) {
			}
		}
		while (true) {
			try {
				compactService.join();
				break;
			} catch (InterruptedException e) {
			}
		}
		LockManager.INSTANCE.versionWriteLock();
		try {
			CommitLog.INSTANCE.shutdown();
		} finally {
			LockManager.INSTANCE.versionWriteUnLock();
		}
		LockManager.INSTANCE.shutdown();
	}

	// private boolean debug = false;

	private void cleanupReaders() {
		SSTableMetaKey delMetaKey = null;
		while (null != (delMetaKey = (SSTableMetaKey) delMetaQueue.poll())) {
			ISSTableReader reader = readers.remove(delMetaKey);
			if (reader != null) {
				if (!conf.debugMode() && reader.meta.markAsDel.get()) {
					SSTableWriter.dataFile(conf.getIndexDir(), reader.meta).delete();
					SSTableWriter.idxFile(conf.getIndexDir(), reader.meta).delete();
					SSTableWriter.dirMetaFile(conf.getIndexDir(), reader.meta).delete();
					logger.info("delete data of " + delMetaKey.version + " " + delMetaKey.level);
				}
			}
		}
	}

	public ISSTableReader getSSTableReader(VersionSet snapshot, SSTableMeta meta) throws IOException {
		cleanupReaders();
		if (snapshot.curTable == null) {
			return null;
		} else if (snapshot.curTable.getMeta() == meta) {
			return snapshot.curTable.getReader();
		} else {
			for (MemTable table : snapshot.flushingTables) {
				if (table.getMeta() == meta) {
					return table.getReader();
				}
			}
			return getDiskSSTableReader(meta);
		}
	}

	ISSTableReader getDiskSSTableReader(SSTableMeta meta) throws IOException {
		cleanupReaders();
		ISSTableReader ret = null;
		SSTableMetaKey ref = new SSTableMetaKey(meta, delMetaQueue);
		if (readers.containsKey(ref)) {
			ret = readers.get(ref);
		} else {
			ret = new DiskSSTableReader(this, meta.clone());
			ISSTableReader tmp = readers.putIfAbsent(ref, ret);
			if (tmp != null)
				ret = tmp;
		}
		ret.init();
		return ret;
	}
}
