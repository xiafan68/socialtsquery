package core.lsmt.bdbindex;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
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

import core.io.Block;
import core.io.Bucket;
import core.io.Bucket.BucketID;
import core.io.SeekableDirectIO;
import core.lsmo.bdbformat.OctreeSSTableWriter;
import core.lsmt.FileBasedIndexHelper;
import core.lsmt.IBucketBasedSSTableReader;
import core.lsmt.IMemTable.SSTableMeta;
import core.lsmt.ISSTableWriter.DirEntry;
import core.lsmt.LSMTInvertedIndex;
import core.lsmt.WritableComparableKey;
import core.lsmt.WritableComparableKey.WritableComparableKeyFactory;
import util.Pair;

/**
 * This implementation stores both directory data and (key, offset) index in a custom file format.
 * 
 * @author xiafan
 *
 */
public abstract class BucketBasedSSTableReader implements IBucketBasedSSTableReader {
	protected SeekableDirectIO dataInput;
	protected SeekableDirectIO dirInput;

	protected Map<WritableComparableKey, DirEntry> dirMap = new TreeMap<WritableComparableKey, DirEntry>();
	protected Map<WritableComparableKey, List> skipList = new HashMap<WritableComparableKey, List>();
	// private Map<WritableComparableKey, Integer> wordFreq = new
	// HashMap<WritableComparableKey, Integer>();

	protected AtomicBoolean init = new AtomicBoolean(false);

	protected LSMTInvertedIndex index;
	protected SSTableMeta meta;
	WritableComparableKeyFactory keyFactory;

	public BucketBasedSSTableReader(LSMTInvertedIndex index, SSTableMeta meta,
			WritableComparableKeyFactory keyFactory) {
		this.index = index;
		this.meta = meta;
		this.keyFactory = keyFactory;
	}

	public DirEntry getDirEntry(WritableComparableKey key) {
		return dirMap.get(key);
	}

	public boolean isInited() {
		return true;
	}

	public void init() throws IOException {
		if (!init.get()) {
			synchronized (this) {
				if (!init.get()) {
					File dataDir = index.getConf().getIndexDir();
					dataInput = SeekableDirectIO.create(OctreeSSTableWriter.dataFile(dataDir, meta).getAbsolutePath());
					// loadStats();
					loadDirMeta();
					loadIndex();
				}
				init.set(true);
			}
		}
	}

	private void loadDirMeta() throws IOException {
		File dataDir = index.getConf().getIndexDir();
		FileInputStream fis = new FileInputStream(FileBasedIndexHelper.dirMetaFile(dataDir, meta));
		DataInputStream dirInput = new DataInputStream(fis);
		DirEntry entry = null;
		while (dirInput.available() > 0) {
			entry = new DirEntry(index.getConf().getIndexKeyFactory());
			entry.read(dirInput);
			dirMap.put(entry.curKey, entry);
		}
	}

	private void loadIndex() throws IOException {
		// load index
		File dataDir = index.getConf().getIndexDir();
		FileInputStream fis = new FileInputStream(FileBasedIndexHelper.idxFile(dataDir, meta));
		DataInputStream indexDis = new DataInputStream(fis);
		try {
			WritableComparableKey curCode = null;
			BucketID buck = null;
			List<Pair<WritableComparableKey, BucketID>> curList = null;
			for (Entry<WritableComparableKey, DirEntry> entry : dirMap.entrySet()) {
				if (!skipList.containsKey(entry.getKey())) {
					curList = new ArrayList<Pair<WritableComparableKey, BucketID>>();
					skipList.put(entry.getKey(), curList);
				} else {
					curList = skipList.get(entry.getKey());
				}

				for (int i = 0; i < entry.getValue().sampleNum; i++) {
					curCode = keyFactory.createIndexKey();
					curCode.read(indexDis);
					buck = new BucketID();
					buck.read(indexDis);
					curList.add(new Pair<WritableComparableKey, BucketID>(curCode, buck));
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
	public synchronized int getBucket(BucketID id, Bucket bucket) throws IOException {
		bucket.reset();
		dataInput.seek(id.getFileOffset());
		bucket.read(dataInput);
		return (int) (dataInput.position() / Block.BLOCK_SIZE);
	}

	Comparator<Pair<WritableComparableKey, BucketID>> comp = new Comparator<Pair<WritableComparableKey, BucketID>>() {
		@Override
		public int compare(Pair<WritableComparableKey, BucketID> o1, Pair<WritableComparableKey, BucketID> o2) {
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
	public Pair<WritableComparableKey, BucketID> cellOffset(WritableComparableKey key, WritableComparableKey code)
			throws IOException {
		Pair<WritableComparableKey, BucketID> ret = null;
		if (skipList.containsKey(key)) {
			List<Pair<WritableComparableKey, BucketID>> list = skipList.get(key);
			int idx = Collections.binarySearch(list, new Pair<WritableComparableKey, BucketID>(code, null), comp);
			if (idx < 0) {
				idx = Math.abs(idx + 1);
				idx = (idx > 0) ? idx - 1 : 0;
			}
			ret = list.get(idx);
		}
		return ret;
	}

	public Pair<WritableComparableKey, BucketID> floorOffset(WritableComparableKey curKey,
			WritableComparableKey curCode) {
		Pair<WritableComparableKey, BucketID> ret = null;
		if (skipList.containsKey(curKey)) {
			List<Pair<WritableComparableKey, BucketID>> list = skipList.get(curKey);
			int idx = Collections.binarySearch(list, new Pair<WritableComparableKey, BucketID>(curCode, null), comp);
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
	public Iterator<WritableComparableKey> keySetIter() {
		return dirMap.keySet().iterator();
	}

	@Override
	public void close() throws IOException {
		if (dataInput != null)
			dataInput.close();
		if (dirInput != null)
			dirInput.close();

		skipList.clear();
		dirMap.clear();
	}
}
