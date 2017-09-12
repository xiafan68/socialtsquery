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
import core.lsmt.WritableComparable;
import core.lsmt.WritableComparable.WritableComparableKeyFactory;
import util.Pair;
import util.Profile;
import util.ProfileField;

/**
 * This implementation stores both directory data and (key, offset) index in a
 * custom file format.
 * 
 * @author xiafan
 *
 */
public abstract class BucketBasedSSTableReader implements IBucketBasedSSTableReader {
	protected SeekableDirectIO dataInput;
	protected SeekableDirectIO dirInput;

	protected Map<WritableComparable, DirEntry> dirMap = new TreeMap<WritableComparable, DirEntry>();
	protected Map<WritableComparable, List> skipList = new HashMap<WritableComparable, List>();
	// private Map<WritableComparableKey, Integer> wordFreq = new
	// HashMap<WritableComparableKey, Integer>();

	protected AtomicBoolean init = new AtomicBoolean(false);

	protected LSMTInvertedIndex index;
	protected SSTableMeta meta;
	// the factory to create key of SkipList, which is our encoding
	WritableComparableKeyFactory keyFactory;

	Comparator<Pair<WritableComparable, BucketID>> comp = new Comparator<Pair<WritableComparable, BucketID>>() {
		@Override
		public int compare(Pair<WritableComparable, BucketID> o1, Pair<WritableComparable, BucketID> o2) {
			return o1.getKey().compareTo(o2.getKey());
		}
	};

	public BucketBasedSSTableReader(LSMTInvertedIndex index, SSTableMeta meta,
			WritableComparableKeyFactory keyFactory) {
		this.index = index;
		this.meta = meta;
		this.keyFactory = keyFactory;
	}

	public DirEntry getDirEntry(WritableComparable key) {
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
			WritableComparable curCode = null;
			BucketID buck = null;
			List<Pair<WritableComparable, BucketID>> curList = null;
			for (Entry<WritableComparable, DirEntry> entry : dirMap.entrySet()) {
				if (!skipList.containsKey(entry.getKey())) {
					curList = new ArrayList<Pair<WritableComparable, BucketID>>();
					skipList.put(entry.getKey(), curList);
				} else {
					curList = skipList.get(entry.getKey());
				}

				for (int i = 0; i < entry.getValue().sampleNum; i++) {
					curCode = keyFactory.createIndexKey();
					curCode.read(indexDis);
					buck = new BucketID();
					buck.read(indexDis);
					curList.add(new Pair<WritableComparable, BucketID>(curCode, buck));
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

	@Override
	public synchronized int getBlockFromDataFile(Block block) throws IOException {
		Profile.instance.start(ProfileField.READ_BLOCK.toString());
		try {
			dataInput.seek(block.getFileOffset());
			block.read(dataInput);
			return (int) (dataInput.position() / Block.BLOCK_SIZE);
		} finally {
			Profile.instance.end(ProfileField.READ_BLOCK.toString());
		}
	}

	/**
	 * 找到第一个不大于key, code的octant的offset
	 * 
	 * @param key
	 * @param code
	 * @return
	 * @throws IOException
	 */
	public BucketID cellOffset(WritableComparable key, WritableComparable code) throws IOException {
		Pair<WritableComparable, BucketID> ret = null;
		if (skipList.containsKey(key)) {
			List<Pair<WritableComparable, BucketID>> list = skipList.get(key);
			int idx = Collections.binarySearch(list, new Pair<WritableComparable, BucketID>(code, null), comp);
			if (idx < 0) {
				idx = Math.abs(idx + 1);
				idx = (idx > 0) ? idx - 1 : 0;
			}
			ret = list.get(idx);
		}
		if (ret != null)
			return ret.getValue();
		return null;
	}

	public BucketID floorOffset(WritableComparable curKey, WritableComparable curCode) {
		Pair<WritableComparable, BucketID> ret = null;
		if (skipList.containsKey(curKey)) {
			List<Pair<WritableComparable, BucketID>> list = skipList.get(curKey);
			int idx = Collections.binarySearch(list, new Pair<WritableComparable, BucketID>(curCode, null), comp);
			if (idx < 0) {
				idx = Math.abs(idx + 1);
			}
			if (idx < list.size())
				ret = list.get(idx);
		}
		if (ret != null)
			return ret.getValue();
		return null;
	}

	@Override
	public SSTableMeta getMeta() {
		return meta;
	}

	@Override
	public Iterator<WritableComparable> keySetIter() {
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
