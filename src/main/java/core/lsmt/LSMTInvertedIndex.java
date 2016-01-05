package core.lsmt;

import java.io.File;
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
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import common.MidSegment;
import core.commom.TempKeywordQuery;
import core.executor.IQueryExecutor;
import core.executor.QueryExecutorFactory;
import core.lsmo.octree.MemoryOctreeIterator;
import core.lsmo.octree.OctreeNode;
import core.lsmo.octree.OctreeNode.CompressedSerializer;
import core.lsmo.octree.OctreePostingListIter;
import core.lsmt.IMemTable.SSTableMeta;
import core.lsmt.WritableComparableKey.StringKey;
import segmentation.Interval;
import shingle.TextShingle;
import util.Configuration;
import util.Profile;
import util.ProfileField;

/**
 * 
 * @author xiafan
 *
 */
public class LSMTInvertedIndex {
	private static final Logger logger = Logger.getLogger(LSMTInvertedIndex.class);

	// AtomicBoolean running = new AtomicBoolean(true);
	final ILSMTFactory implFactory;
	File dataDir;
	boolean bootstrap = true;
	volatile boolean stop = false;
	IMemTable curTable;
	volatile VersionSet versionSet = new VersionSet();
	int nextVersion = 0;

	LockManager lockManager;
	FlushService flushService;
	CompactService compactService;
	Configuration conf;
	TextShingle shingle = new TextShingle(null);

	ISSTableWriter valWriter;

	// DataOutputStream keyWriter;

	public LSMTInvertedIndex(Configuration conf) {
		this.conf = conf;

		try {
			this.implFactory = (ILSMTFactory) Enum.valueOf(Class.forName(conf.getIndexFactory()).asSubclass(Enum.class),
					"INSTANCE");
		} catch (ClassNotFoundException e) {
			throw new RuntimeException(e);
		}
		valWriter = implFactory.newSSTableWriterForCompaction(null, new ArrayList<ISSTableReader>(), conf);
	}

	public LockManager getLockManager() {
		return lockManager;
	}

	/**
	 * TODO: 1. 扫描磁盘文件，确定一个版本集合。2. 启动写出服务;3. 启动日志服务，开始日志恢复;4. 启动压缩服务，开始接受更新与查询
	 * 
	 * @throws IOException
	 */
	public void init() throws IOException {
		// setup serializer for octreenode!!!
		OctreeNode.HANDLER = CompressedSerializer.INSTANCE;
		bootstrap = true;
		lockManager = new LockManager();
		lockManager.setBootStrap();

		flushService = new FlushService(this);
		flushService.start();

		setupVersionSet();

		CommitLog.INSTANCE.init(conf);
		CommitLog.INSTANCE.recover(this);
		CommitLog.INSTANCE.openNewLog(curTable.getMeta().version);
		compactService = new CompactService(this);
		if (conf.shouldCompact())
			compactService.start();
		lockManager.setBootStrap();
		bootstrap = false;

	}

	private static final Pattern regex = Pattern.compile("[0-9]+_[0-9]+.data");

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

		List<File> exactVersion = new ArrayList<File>();
		for (File file : indexFiles) {
			int[] v1 = parseVersion(file);
			for (int i = exactVersion.size() - 1; i >= 0; i--) {
				File cur = exactVersion.get(i);
				int[] curV = parseVersion(cur);
				if (curV[1] < v1[1]) {
					exactVersion.remove(i);
					delIndexFile(new SSTableMeta(curV[0], curV[1]));
				} else {
					break;
				}
			}
			exactVersion.add(file);
		}

		nextVersion = 0;
		for (File file : exactVersion) {
			int[] version = parseVersion(file);
			SSTableMeta meta = new SSTableMeta(version[0], version[1]);
			if (validateDataFile(meta)) {
				versionSet.diskTreeMetas.add(meta);
				nextVersion = Math.max(nextVersion, meta.version + 1);
			}
		}
		SSTableMeta meta = new SSTableMeta(nextVersion++, 0);
		curTable = implFactory.newMemTable(this, meta);
		versionSet.newMemTable(curTable);
	}

	/**
	 * 验证是否所有的索引文件都存在
	 * 
	 * @param meta
	 * @return
	 */
	private boolean validateDataFile(SSTableMeta meta) {
		boolean ret = valWriter.validate(meta);
		if (!ret) {
			logger.error("index file of version " + meta + " is not consistent");
		}
		return ret;
	}

	public ILSMTFactory getLSMTFactory() {
		return implFactory;
	}

	public static int[] parseVersion(File file) {
		int[] ret = new int[2];
		String[] field = file.getName().split("_");
		ret[0] = Integer.parseInt(field[0]);
		ret[1] = Integer.parseInt(field[1].substring(0, field[1].indexOf('.')));
		return ret;
	}

	/*
	 * Map<String, Integer> keyCode = new HashMap<String, Integer>();
	 * 
	 * public synchronized int getKeywordCode(String keyword) throws IOException
	 * { if (!keyCode.containsKey(keyword)) { keyCode.put(keyword,
	 * keyCode.size()); keyWriter.writeUTF(keyword);
	 * keyWriter.writeInt(keyCode.get(keyword)); } return keyCode.get(keyword);
	 * }
	 */

	public void insert(List<String> keywords, MidSegment seg) throws IOException {
		lockManager.versionReadLock();
		try {
			for (String keyword : keywords) {
				if (!bootstrap)
					CommitLog.INSTANCE.write(keyword, seg);
				// int code = getKeywordCode(keyword);
				lockManager.postWriteLock(keyword.hashCode());
				try {
					curTable.insert(new StringKey(keyword), seg);
				} finally {
					lockManager.postWriteUnLock(keyword.hashCode());
				}
			}
		} finally {
			lockManager.versionReadUnLock();
		}
		maySwitchMemtable();
	}

	public void insert(String keywords, MidSegment seg) throws IOException {
		insert(shingle.shingling(keywords), seg);
	}

	// TODO check whether we need to switch the memtable
	public void maySwitchMemtable() {
		IMemTable tmp = curTable;
		if (tmp.size() > conf.getFlushLimit() || System.currentTimeMillis() - tmp.createAt() > conf.getDurationTime()) {
			lockManager.versionWriteLock();
			try {
				// check for violation again
				if (tmp == curTable) {
					// open new log
					CommitLog.INSTANCE.openNewLog(nextVersion);
					SSTableMeta meta = new SSTableMeta(nextVersion++, 0);
					tmp = implFactory.newMemTable(this, meta);
					versionSet = versionSet.clone();
					versionSet.newMemTable(tmp);
					curTable = tmp;
				}
			} finally {
				lockManager.versionWriteUnLock();
			}
		}

		// block until data have been flushed
		while (versionSet.flushingTables.size() >= 5) {
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
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
		lockManager.versionWriteLock();
		versionSet = versionSet.clone();
		versionSet.flush(versions, newMeta);

		// mark redo logs as deleted
		for (SSTableMeta version : versions)
			CommitLog.INSTANCE.deleteLog(version.version);
		lockManager.versionWriteUnLock();
	}

	public void compactTables(Set<SSTableMeta> versions, SSTableMeta newMeta) {
		logger.info("compacted versions for " + versions);
		lockManager.versionWriteLock();
		versionSet = versionSet.clone();
		versionSet.compact(versions, newMeta);
		lockManager.versionWriteUnLock();
	}

	/**
	 * 
	 * @return
	 */
	public ILSMTFactory getFactory() {
		return implFactory;
	}

	public VersionSet getVersion() {
		return versionSet;
	}

	public Iterator<Interval> query(String query, int start, int end, int k, String execType) throws IOException {
		return query(shingle.shingling(query), start, end, k, execType);
	}

	public Iterator<Interval> query(List<String> keywords, int start, int end, int k, String execType)
			throws IOException {
		logger.info("querying keywords:" + StringUtils.join(keywords, ","));
		Profile.instance.start(ProfileField.TOTAL_TIME.toString());
		try {
			start -= conf.queryStartTime();
			end -= conf.queryStartTime();
			IQueryExecutor exec = QueryExecutorFactory.createExecutor(this, execType);
			String[] wordArr = new String[keywords.size()];
			keywords.toArray(wordArr);
			exec.setMaxLifeTime(60 * 60 * 24 * 365 * 10);
			exec.query(new TempKeywordQuery(wordArr, new Interval(-1, start, end, 0), k));
			return exec.getAnswer();
		} finally {
			Profile.instance.end(ProfileField.TOTAL_TIME.toString());
		}
	}

	public Map<String, IPostingListIterator> getPostingListIter(List<String> keywords, int start, int end)
			throws IOException {
		Map<String, IPostingListIterator> ret = new HashMap<String, IPostingListIterator>();
		for (String keyword : keywords) {
			PostingListMergeView view = new PostingListMergeView();
			// int key = getKeywordCode(keyword);
			// add iter for current memtable
			view.addIterator(versionSet.curTable.getReader().getPostingListIter(new StringKey(keyword), start, end));
			// add iter for flushing memtable
			for (IMemTable table : versionSet.flushingTables) {
				view.addIterator(table.getReader().getPostingListIter(new StringKey(keyword), start, end));
			}
			for (SSTableMeta meta : versionSet.diskTreeMetas) {
				ISSTableReader reader = getSSTableReader(versionSet, meta);
				view.addIterator(reader.getPostingListIter(new StringKey(keyword), start, end));
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
		public IMemTable curTable = null;
		public List<IMemTable> flushingTables = new ArrayList<IMemTable>();
		public List<SSTableMeta> diskTreeMetas = new ArrayList<SSTableMeta>();

		// public HashMap<OctreeMeta>

		/**
		 * client grantuee thread safe, used when flushing current octree
		 * 
		 * @param newTree
		 */
		public void newMemTable(IMemTable newTable) {
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
			Iterator<IMemTable> iter = flushingTables.iterator();
			while (iter.hasNext()) {
				IMemTable table = iter.next();
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

		@Override
		public String toString() {
			StringBuffer ret = new StringBuffer("VersionSet [diskTreeMetas=" + diskTreeMetas + ",");
			if (curTable != null)
				ret.append("cur memtable:" + curTable.getMeta());
			ret.append("flushing memtables:");
			for (IMemTable table : flushingTables) {
				ret.append(table.getMeta());
				ret.append(",");
			}
			return ret.toString();
		}
	}

	public void close() throws IOException {
		// keyWriter.close();
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
		for (ISSTableReader reader : readers.values()) {
			reader.close();
		}
		lockManager.versionWriteLock();
		try {
			CommitLog.INSTANCE.shutdown();
		} finally {
			lockManager.versionWriteUnLock();
		}
		lockManager.shutdown();
	}

	// private boolean debug = false;

	private void cleanupReaders() {
		SSTableMetaKey delMetaKey = null;
		while (null != (delMetaKey = (SSTableMetaKey) delMetaQueue.poll())) {
			ISSTableReader reader = readers.remove(delMetaKey);
			if (reader != null) {
				try {
					reader.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
				if (!conf.debugMode() && reader.getMeta().markAsDel.get()) {
					delIndexFile(reader.getMeta());
				}
			}
		}
	}

	private void delIndexFile(SSTableMeta meta) {
		valWriter.delete(conf.getIndexDir(), meta);
		logger.info("delete data of " + meta.version + " " + meta.level);
	}

	public ISSTableReader getSSTableReader(VersionSet snapshot, SSTableMeta meta) throws IOException {
		cleanupReaders();
		if (snapshot.curTable == null) {
			return null;
		} else if (snapshot.curTable.getMeta() == meta) {
			return snapshot.curTable.getReader();
		} else {
			for (IMemTable table : snapshot.flushingTables) {
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
			ret = implFactory.newSSTableReader(this, meta.clone());
			ISSTableReader tmp = readers.putIfAbsent(ref, ret);
			if (tmp != null)
				ret = tmp;
		}
		ret.init();
		return ret;
	}

	/**
	 * TODO:当前索引对应的分区信息，这个分区是按照生命周期进行的分区
	 * 
	 * @return
	 */
	public List<PartitionMeta> getPartitions() {
		return Arrays.asList(new PartitionMeta((int) (Math.log(Integer.MAX_VALUE / 4) / Math.log(2))));
	}

	/**
	 * 统计每个posting list中leafnode的比例的分布
	 * 
	 * @return
	 */
	public HashMap<String, List> collectStatistics() {
		return null;
	}

	public int getPostingListSize(String keyword) throws IOException {
		StringKey key = new StringKey(keyword);

		VersionSet curSet = versionSet;
		int size = 0;
		if (curSet.curTable.getPostingList(key) != null) {
			size += curSet.curTable.getPostingList(key).size();
		}

		for (IMemTable table : versionSet.flushingTables) {
			if (table.getPostingList(key) != null) {
				size += table.getPostingList(key).size();
			}
		}

		for (SSTableMeta meta : versionSet.diskTreeMetas) {
			ISSTableReader reader = getSSTableReader(versionSet, meta);
			if (reader.getDirEntry(key) != null) {
				size += reader.getDirEntry(key).size;
			}
		}

		return size;
	}

	public int getPostingListSizeByExec(String keyword) throws IOException {
		StringKey key = new StringKey(keyword);

		VersionSet curSet = versionSet;
		int size = 0;
		if (curSet.curTable.getPostingList(key) != null) {
			size += curSet.curTable.getPostingList(key).size();
		}

		for (IMemTable table : versionSet.flushingTables) {
			if (table.getPostingList(key) != null) {
				size += table.getPostingList(key).size();
			}
		}

		for (SSTableMeta meta : versionSet.diskTreeMetas) {
			if (!meta.markAsDel.get()) {
				ISSTableReader reader = getSSTableReader(versionSet, meta);
				IPostingListIterator scanner = reader.getPostingListScanner(key);
				while (scanner.hasNext()) {
					size += scanner.next().getValue().size();
				}
			}
		}

		return size;
	}

	/**
	 * 统计每个posting list中leafnode的比例的分布
	 * 
	 * @return
	 * @throws IOException
	 */
	public Map<Integer, Integer> collectStatistics(String term) throws IOException {
		List<IPostingListIterator> iters = new ArrayList<IPostingListIterator>();

		iters.add(versionSet.curTable.getReader().getPostingListIter(new StringKey(term), 0, Integer.MAX_VALUE));
		Map<Integer, Integer> dist = new TreeMap<Integer, Integer>();

		for (SSTableMeta meta : versionSet.diskTreeMetas) {
			ISSTableReader reader = getSSTableReader(versionSet, meta);
			iters.add(reader.getPostingListIter(new StringKey(term), 0, Integer.MAX_VALUE));
		}

		for (IPostingListIterator iter : iters) {
			while (iter.hasNext()) {
				OctreeNode node = null;
				if (iter instanceof MemoryOctreeIterator) {
					node = ((MemoryOctreeIterator) iter).nextNode();
				} else {
					node = ((OctreePostingListIter) iter).nextNode();
				}

				if (node.getEncoding().getEdgeLen() != 1) {
					int[] hist = node.histogram();
					if (hist[0] + hist[1] != 0) {
						int ratio = (hist[0] + 1) / (hist[1] + 1);
						if (!dist.containsKey(ratio))
							dist.put(ratio, 1);
						else
							dist.put(ratio, dist.get(ratio) + 1);
					}
				}
			}
			iter.close();
		}
		return dist;
	}
}
