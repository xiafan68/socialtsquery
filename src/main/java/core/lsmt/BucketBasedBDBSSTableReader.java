package core.lsmt;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Comparator;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;

import Util.Pair;
import core.commom.BDBBtree;
import core.io.Bucket;
import core.io.Bucket.BucketID;
import core.lsmo.OctreeSSTableWriter;
import core.lsmt.IMemTable.SSTableMeta;
import core.lsmt.WritableComparableKey.WritableComparableKeyFactory;

/**
 * 基于bucket的reader公共类，主要有三个文件： 1. index file:记录的是key-bucketid对 2. dir
 * file:记录当前对应的目录信息 3. data file:
 * 
 * @author xiafan
 *
 */
public abstract class BucketBasedBDBSSTableReader implements ISSTableReader {
	protected RandomAccessFile dataInput;
	//protected RandomAccessFile dirInput;
	// protected RandomAccessFile idxInput;

	protected BDBBtree dirMap = null;
	protected BDBBtree skipList = null;
	// private Map<WritableComparableKey, Integer> wordFreq = new
	// HashMap<WritableComparableKey, Integer>();

	protected AtomicBoolean init = new AtomicBoolean(false);

	protected LSMTInvertedIndex index;
	protected SSTableMeta meta;
	WritableComparableKeyFactory keyFactory;

	public BucketBasedBDBSSTableReader(LSMTInvertedIndex index,
			SSTableMeta meta, WritableComparableKeyFactory keyFactory) {
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
					// loadStats();
					loadDirMeta();
					loadIndex();
				}
				init.set(true);
			}
		}
	}

	/*private void loadStats() throws IOException {
		File dataDir = index.getConf().getIndexDir();
		FileInputStream fis = new FileInputStream(
				OctreeSSTableWriter.dirMetaFile(dataDir, meta));
		DataInputStream dirInput = new DataInputStream(new BufferedInputStream(
				fis));
		WritableComparableKeyFactory factory = index.getConf().getIndexKeyFactory();
		while (dirInput.available() > 0) {
			WritableComparableKey key = factory.createIndexKey();
			key.read(dirInput);
			int count = dirInput.readInt();
			wordFreq.put(key, count);
		}
	}*/

	private void loadDirMeta() throws IOException {
		// TODO
	}

	private void loadIndex() throws IOException {
		// TODO
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

	Comparator<Pair<WritableComparableKey, BucketID>> comp = new Comparator<Pair<WritableComparableKey, BucketID>>() {
		@Override
		public int compare(Pair<WritableComparableKey, BucketID> o1,
				Pair<WritableComparableKey, BucketID> o2) {
			return o1.getKey().compareTo(o2.getKey());
		}

	};

	/**
	 * 找到第一个key相同，不大于code的octant的offset,
	 * 
	 * @param curKey
	 * @param curCode
	 * @return
	 * @throws IOException
	 */
	public Pair<WritableComparableKey, BucketID> cellOffset(
			WritableComparableKey curKey, WritableComparableKey curCode)
			throws IOException {
		return skipList.floorOffset(curKey, curCode);
	}

	public Pair<WritableComparableKey, BucketID> floorOffset(
			WritableComparableKey curKey, WritableComparableKey curCode) throws IOException {
		return skipList.cellOffset(curKey, curCode);
	}

	@Override
	public SSTableMeta getMeta() {
		return meta;
	}

	@Override
	public Iterator<WritableComparableKey> keySetIter() {
		return dirMap.keyIterator();
	}

	@Override
	public void close() throws IOException {
		if (dataInput != null)
			dataInput.close();
		//if (dirInput != null)
		//	dirInput.close();

		skipList.close();
		dirMap.close();
	}
}
