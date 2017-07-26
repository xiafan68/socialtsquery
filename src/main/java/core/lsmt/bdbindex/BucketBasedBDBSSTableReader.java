package core.lsmt.bdbindex;

import java.io.File;
import java.io.IOException;
import java.util.Comparator;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;

import core.commom.BDBBtree;
import core.io.Block;
import core.io.Bucket;
import core.io.Bucket.BucketID;
import core.io.SeekableDirectIO;
import core.lsmo.bdbformat.OctreeSSTableWriter;
import core.lsmt.IBucketBasedSSTableReader;
import core.lsmt.IMemTable.SSTableMeta;
import core.lsmt.ISSTableWriter.DirEntry;
import core.lsmt.LSMTInvertedIndex;
import core.lsmt.WritableComparableKey;
import core.lsmt.WritableComparableKey.WritableComparableKeyFactory;
import util.Pair;
import util.Profile;
import util.ProfileField;

/**
 *This implementation stores both directory data and offset index in Berkeley db
 * 
 * @author xiafan
 *
 */
public abstract class BucketBasedBDBSSTableReader implements IBucketBasedSSTableReader {
	protected SeekableDirectIO dataInput;
	protected BDBBtree dirMap = null;
	protected BDBBtree skipList = null;

	protected AtomicBoolean init = new AtomicBoolean(false);

	protected LSMTInvertedIndex index;
	protected SSTableMeta meta;
	WritableComparableKeyFactory keyFactory;

	public BucketBasedBDBSSTableReader(LSMTInvertedIndex index, SSTableMeta meta,
			WritableComparableKeyFactory keyFactory) {
		this.index = index;
		this.meta = meta;
		this.keyFactory = keyFactory;
	}

	public boolean isInited() {
		return true;
	}

	@Override
	public DirEntry getDirEntry(WritableComparableKey key) throws IOException {
		return dirMap.get(key);
	}

	public void init() throws IOException {
		if (!init.get()) {
			synchronized (this) {
				if (!init.get()) {
					File dataDir = index.getConf().getIndexDir();
					dataInput = SeekableDirectIO.create(OctreeSSTableWriter.dataFile(dataDir, meta), "r");
					// loadStats();
					loadDirMeta();
					loadIndex();
				}
				init.set(true);
			}
		}
	}

	private void loadDirMeta() throws IOException {
		dirMap = new BDBBtree(BDBBasedIndexHelper.dirMetaFile(index.getConf().getIndexDir(), meta), index.getConf());
		dirMap.open(false, false);
	}

	private void loadIndex() throws IOException {
		skipList = new BDBBtree(BDBBasedIndexHelper.idxFile(index.getConf().getIndexDir(), meta), index.getConf());
		skipList.open(false, true);
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
		Profile.instance.start(ProfileField.READ_BLOCK.toString());
		try {
			bucket.reset();
			bucket.setBlockIdx(id.blockID);
			dataInput.seek(id.getFileOffset());
			bucket.read(dataInput);
			return (int) (dataInput.position() / Block.BLOCK_SIZE);
		} finally {
			Profile.instance.end(ProfileField.READ_BLOCK.toString());
		}
	}

	Comparator<Pair<WritableComparableKey, BucketID>> comp = new Comparator<Pair<WritableComparableKey, BucketID>>() {
		@Override
		public int compare(Pair<WritableComparableKey, BucketID> o1, Pair<WritableComparableKey, BucketID> o2) {
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
	public Pair<WritableComparableKey, BucketID> cellOffset(WritableComparableKey curKey, WritableComparableKey curCode)
			throws IOException {
		Profile.instance.start("celloffset");
		try {
			return skipList.floorOffset(curKey, curCode);
		} finally {
			Profile.instance.end("celloffset");
		}
	}

	public Pair<WritableComparableKey, BucketID> floorOffset(WritableComparableKey curKey,
			WritableComparableKey curCode) throws IOException {
		Profile.instance.start("celloffset");
		try {
			return skipList.cellOffset(curKey, curCode);
		} finally {
			Profile.instance.end("celloffset");
		}
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
		// if (dirInput != null)
		// dirInput.close();

		skipList.close();
		dirMap.close();
	}
}
