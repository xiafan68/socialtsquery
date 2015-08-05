package core.index;

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
import core.commom.Encoding;
import core.index.MemTable.SSTableMeta;
import core.index.SSTableWriter.DirEntry;
import core.index.octree.DiskOctreeIterator;
import core.index.octree.IOctreeIterator;
import core.index.octree.OctreePostingListIter;
import core.io.Bucket;
import core.io.Bucket.BucketID;

/**
 * This class provides interfaces to locate a posting list given the keyword,
 * locate the position given the octree code and the interface to read the
 * bucket.
 * 
 * @author xiafan
 *
 */
public class DiskSSTableReader extends ISSTableReader {
	RandomAccessFile dataInput;
	RandomAccessFile dirInput;

	Map<Integer, DirEntry> dirMap = new TreeMap<Integer, DirEntry>();
	Map<Integer, List> skipList = new HashMap<Integer, List>();
	AtomicBoolean init = new AtomicBoolean(false);

	public DiskSSTableReader(LSMOInvertedIndex index, SSTableMeta meta) {
		super(index, meta);
	}

	public void init() throws IOException {
		if (!init.get()) {
			synchronized (this) {
				if (!init.get()) {
					File dataDir = index.getConf().getIndexDir();
					dataInput = new RandomAccessFile(SSTableWriter.dataFile(dataDir, meta), "r");
					loadDirMeta();
					loadIndex();
				}
			}
			init.set(true);
		}
	}

	private void loadDirMeta() throws IOException {
		File dataDir = index.getConf().getIndexDir();
		FileInputStream fis = new FileInputStream(SSTableWriter.dirMetaFile(dataDir, meta));
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
		FileInputStream fis = new FileInputStream(SSTableWriter.idxFile(dataDir, meta));
		DataInputStream indexDis = new DataInputStream(fis);
		try {
			Encoding curCode = null;
			BucketID buck = null;
			List<Pair<Encoding, BucketID>> curList = null;
			for (Entry<Integer, DirEntry> entry : dirMap.entrySet()) {
				if (!skipList.containsKey(entry.getKey())) {
					curList = new ArrayList<Pair<Encoding, BucketID>>();
					skipList.put(entry.getKey(), curList);
				} else {
					curList = skipList.get(entry.getKey());
				}

				for (int i = 0; i < entry.getValue().sampleNum; i++) {
					curCode = new Encoding();
					curCode.readFields(indexDis);
					buck = new BucketID();
					buck.read(indexDis);
					curList.add(new Pair<Encoding, BucketID>(curCode, buck));
				}
			}
		} finally {
			fis.close();
			indexDis.close();
		}
	}

	public IOctreeIterator getPostingListScanner(int key) {
		return new DiskOctreeIterator(dirMap.get(key), this);
	}

	public IOctreeIterator getPostingListIter(int key, int start, int end) throws IOException {
		IOctreeIterator iter = new OctreePostingListIter(dirMap.get(key), this, start, end);
		iter.open();
		return iter;
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
		return (int) (dataInput.getChannel().position() / Bucket.BLOCK_SIZE);
	}

	/**
	 * 
	 */
	Comparator<Pair<Encoding, BucketID>> comp = new Comparator<Pair<Encoding, BucketID>>() {
		@Override
		public int compare(Pair<Encoding, BucketID> o1, Pair<Encoding, BucketID> o2) {
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
	public Pair<Encoding, BucketID> cellOffset(int key, Encoding code) throws IOException {
		Pair<Encoding, BucketID> ret = null;
		if (skipList.containsKey(key)) {
			List<Pair<Encoding, BucketID>> list = skipList.get(key);
			int idx = Collections.binarySearch(list, new Pair<Encoding, BucketID>(code, null), comp);
			if (idx < 0) {
				idx = Math.abs(idx + 1);
				idx = (idx > 0) ? idx - 1 : 0;
			}
			ret = list.get(idx);
		}
		return ret;
	}

	public Pair<Encoding, BucketID> floorOffset(int curKey, Encoding curCode) {
		Pair<Encoding, BucketID> ret = null;
		if (skipList.containsKey(curKey)) {
			List<Pair<Encoding, BucketID>> list = skipList.get(curKey);
			int idx = Collections.binarySearch(list, new Pair<Encoding, BucketID>(curCode, null), comp);
			if (idx < 0) {
				idx = Math.abs(idx + 1);
			}
			if (idx < list.size())
				ret = list.get(idx);
		}
		return ret;
	}

	@Override
	public Iterator<Integer> keySetIter() {
		return dirMap.keySet().iterator();
	}
}
