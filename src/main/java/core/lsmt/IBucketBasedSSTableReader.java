package core.lsmt;

import java.io.File;
import java.io.IOException;
import java.util.Comparator;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;

import core.commom.BDBBTreeBuilder;
import core.commom.BDBBtree;
import core.commom.IndexFileUtils;
import core.commom.WritableComparableKey;
import core.io.Block;
import core.io.Bucket;
import core.io.Bucket.BucketID;
import core.io.SeekableDirectIO;
import core.lsmo.persistence.DiskOctreeScanner;
import core.lsmt.IMemTable.SSTableMeta;
import core.lsmt.postinglist.IPostingListIterator;
import core.lsmt.postinglist.ISSTableReader;
import util.Pair;
import util.Profile;
import util.ProfileField;

public abstract class IBucketBasedSSTableReader implements ISSTableReader {
	protected SeekableDirectIO dataInput;
	protected SeekableDirectIO markInput;
	protected BDBBtree dirMap = null;

	protected AtomicBoolean init = new AtomicBoolean(false);

	protected LSMTInvertedIndex index;
	protected SSTableMeta meta;

	public IBucketBasedSSTableReader(LSMTInvertedIndex index, SSTableMeta meta) {
		this.index = index;
		this.meta = meta;
	}

	public boolean isInited() {
		return init.get();
	}

	@Override
	public DirEntry getDirEntry(WritableComparableKey key) throws IOException {
		return (DirEntry) dirMap.get(key);
	}

	public void init() throws IOException {
		if (!init.get()) {
			synchronized (this) {
				if (!init.get()) {
					File dataDir = index.getConf().getIndexDir();
					dataInput = SeekableDirectIO.create(IndexFileUtils.dataFile(dataDir, meta), "r");
					if (index.getConf().standaloneSentinal())
						markInput = SeekableDirectIO.create(IndexFileUtils.markFile(dataDir, meta), "r");
					loadDirMeta();
					initIndex();
				}
				init.set(true);
			}
		}
	}

	@Override
	public IPostingListIterator getPostingListScanner(WritableComparableKey key) throws IOException {
		return new DiskOctreeScanner(getDirEntry(key), this);
	}

	private void loadDirMeta() throws IOException {
		dirMap = BDBBTreeBuilder.create().setDir(IndexFileUtils.dirMetaFile(index.getConf().getIndexDir(), meta))
				.setKeyFactory(index.getConf().getDirKeyFactory()).setValueFactory(index.getConf().getDirValueFactory())
				.setAllowDuplicates(false).setReadOnly(true).build();
		dirMap.open();
	}

	public abstract void initIndex() throws IOException;

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
	public abstract Pair<WritableComparableKey, BucketID> floorOffset(WritableComparableKey curKey,
			WritableComparableKey curCode) throws IOException;

	public abstract Pair<WritableComparableKey, BucketID> cellOffset(WritableComparableKey curKey,
			WritableComparableKey curCode) throws IOException;

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

		if (markInput != null)
			markInput.close();
		closeIndex();
		dirMap.close();
	}

	public abstract void closeIndex() throws IOException;

	public LSMTInvertedIndex getIndex() {
		return index;
	}

	public synchronized int getBucketFromMarkFile(Bucket block) throws IOException {
		Profile.instance.start("markblock");
		try {
			markInput.seek(block.blockIdx().getFileOffset());
			block.read(markInput);
			return (int) (markInput.position() / Block.BLOCK_SIZE);
		} finally {
			Profile.instance.end("markblock");
		}
	}

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
}
