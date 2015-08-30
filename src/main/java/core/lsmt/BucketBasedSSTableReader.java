package core.lsmt;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;

import Util.Pair;
import core.io.Bucket;
import core.io.Bucket.BucketID;
import core.lsmo.OctreeSSTableWriter;
import core.lsmt.IMemTable.SSTableMeta;
import core.lsmt.ISSTableWriter.DirEntry;
import core.lsmt.IndexKey.IndexKeyFactory;

/**
 * 基于bucket的reader公共类，主要有三个文件：
 * 1. index file:记录的是key-bucketid对
 * 2. dir file:记录当前对应的目录信息
 * 3. data file:
 * @author xiafan
 *
 */
public abstract class BucketBasedSSTableReader implements ISSTableReader {
	protected RandomAccessFile dataInput;
	protected RandomAccessFile dirInput;
	protected RandomAccessFile idxInput;

	protected Map<Integer, DirEntry> dirMap = new TreeMap<Integer, DirEntry>();
	protected Map<Integer, List> skipList = new HashMap<Integer, List>();
	protected AtomicBoolean init = new AtomicBoolean(false);

	protected LSMTInvertedIndex index;
	protected SSTableMeta meta;
	IndexKeyFactory keyFactory;

	public BucketBasedSSTableReader(LSMTInvertedIndex index, SSTableMeta meta,
			IndexKeyFactory keyFactory) {
		this.index = index;
		this.meta = meta;
		this.keyFactory = keyFactory;
	}

	public boolean isInited() {
		return true;
	}

	public void init() throws IOException {
		if (!init.get()) {
			synchronized (this) {
				if (!init.get()) {
					File dataDir = index.getConf().getIndexDir();
					dataInput = new RandomAccessFile(
							OctreeSSTableWriter.dataFile(dataDir, meta), "r");
					loadDirMeta();
					loadIndex();
				}
				init.set(true);
			}
		}
	}

	private void loadDirMeta() throws IOException {
		File dataDir = index.getConf().getIndexDir();
		FileInputStream fis = new FileInputStream(
				OctreeSSTableWriter.dirMetaFile(dataDir, meta));
		DataInputStream dirInput = new DataInputStream(fis);
		DirEntry entry = null;
		while (dirInput.available() > 0) {
			entry = new DirEntry();
			entry.read(dirInput);
			dirMap.put(entry.curKey, entry);
		}
	}

	private void loadIndex() throws IOException {
		// load index
		File dataDir = index.getConf().getIndexDir();
		FileInputStream fis = new FileInputStream(OctreeSSTableWriter.idxFile(
				dataDir, meta));
		DataInputStream indexDis = new DataInputStream(fis);
		try {
			IndexKey curCode = null;
			BucketID buck = null;
			List<Pair<IndexKey, BucketID>> curList = null;
			for (Entry<Integer, DirEntry> entry : dirMap.entrySet()) {
				if (!skipList.containsKey(entry.getKey())) {
					curList = new ArrayList<Pair<IndexKey, BucketID>>();
					skipList.put(entry.getKey(), curList);
				} else {
					curList = skipList.get(entry.getKey());
				}

				for (int i = 0; i < entry.getValue().sampleNum; i++) {
					curCode = keyFactory.createIndexKey();
					curCode.read(indexDis);
					buck = new BucketID();
					buck.read(indexDis);
					curList.add(new Pair<IndexKey, BucketID>(curCode, buck));
				}
			}
		} finally {
			fis.close();
			indexDis.close();
		}
	}

	/**
	 * 读取id对应的bucket
	 * 
	 * @param id
	 * @param bucket
	 * @return the last offset
	 * @throws IOException
	 */
	public synchronized int getBucket(BucketID id, Bucket bucket)
			throws IOException {
		bucket.reset();
		dataInput.seek(id.getFileOffset());
		bucket.read(dataInput);
		return (int) (dataInput.getChannel().position() / Bucket.BLOCK_SIZE);
	}

	Comparator<Pair<IndexKey, BucketID>> comp = new Comparator<Pair<IndexKey, BucketID>>() {
		@Override
		public int compare(Pair<IndexKey, BucketID> o1,
				Pair<IndexKey, BucketID> o2) {
			return o1.getKey().compareTo(o2.getKey());
		}

	};

	/**
	 * 找到第一个不大于key, code的octant的offset
	 * 
	 * @param key
	 * @param code
	 * @return
	 * @throws IOException
	 */
	public Pair<IndexKey, BucketID> cellOffset(int key, IndexKey code)
			throws IOException {
		Pair<IndexKey, BucketID> ret = null;
		if (skipList.containsKey(key)) {
			List<Pair<IndexKey, BucketID>> list = skipList.get(key);
			int idx = Collections.binarySearch(list,
					new Pair<IndexKey, BucketID>(code, null), comp);
			if (idx < 0) {
				idx = Math.abs(idx + 1);
				idx = (idx > 0) ? idx - 1 : 0;
			}
			ret = list.get(idx);
		}
		return ret;
	}

	public Pair<IndexKey, BucketID> floorOffset(int curKey, IndexKey curCode) {
		Pair<IndexKey, BucketID> ret = null;
		if (skipList.containsKey(curKey)) {
			List<Pair<IndexKey, BucketID>> list = skipList.get(curKey);
			int idx = Collections.binarySearch(list,
					new Pair<IndexKey, BucketID>(curCode, null), comp);
			if (idx < 0) {
				idx = Math.abs(idx + 1);
			}
			if (idx < list.size())
				ret = list.get(idx);
		}
		return ret;
	}

	@Override
	public SSTableMeta getMeta() {
		return meta;
	}

	@Override
	public Iterator<Integer> keySetIter() {
		return dirMap.keySet().iterator();
	}

	@Override
	public void close() throws IOException {
		if (dataInput != null)
			dataInput.close();
		if (dirInput != null)
			dirInput.close();
	}
}
