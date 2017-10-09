package core.lsmt;

import java.io.File;
import java.io.IOException;
import java.util.Comparator;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;

import core.commom.BDBBTreeBuilder;
import core.commom.BDBBtree;
import core.commom.IndexFileUtils;
import core.commom.WritableComparable;
import core.io.Block;
import core.io.Bucket;
import core.io.Bucket.BucketID;
import core.io.SeekableDirectIO;
import core.lsmo.persistence.DiskOctreeScanner;
import core.lsmt.IMemTable.SSTableMeta;
import core.lsmt.postinglist.IPostingListIterator;
import core.lsmt.postinglist.ISSTableReader;
import util.Configuration;
import util.Pair;
import util.Profile;
import util.ProfileField;

public abstract class IBucketBasedSSTableReader implements ISSTableReader {
	protected SeekableDirectIO dataInput;
	protected SeekableDirectIO markInput;
	protected BDBBtree dirMap = null;

	protected AtomicBoolean init = new AtomicBoolean(false);

	protected Configuration conf;
	protected SSTableMeta meta;

	public IBucketBasedSSTableReader(Configuration conf, SSTableMeta meta) {
		this.conf = conf;
		this.meta = meta;
	}

	public boolean isInited() {
		return init.get();
	}

	@Override
	public DirEntry getDirEntry(WritableComparable key) throws IOException {
		return (DirEntry) dirMap.get(key);
	}

	public void init() throws IOException {
		if (!init.get()) {
			synchronized (this) {
				if (!init.get()) {
					File dataDir =conf.getIndexDir();
					dataInput = SeekableDirectIO.create(IndexFileUtils.dataFile(dataDir, meta), "r");
					if (conf.standaloneSentinal())
						markInput = SeekableDirectIO.create(IndexFileUtils.markFile(dataDir, meta), "r");
					loadDirMeta();
					initIndex();
				}
				init.set(true);
			}
		}
	}

	@Override
	public IPostingListIterator getPostingListScanner(WritableComparable key) throws IOException {
		return new DiskOctreeScanner(getDirEntry(key), this);
	}

	private void loadDirMeta() throws IOException {
		dirMap = BDBBTreeBuilder.create().setDir(IndexFileUtils.dirMetaFile(conf.getIndexDir(), meta))
				.setKeyFactory(conf.getDirKeyFactory()).setValueFactory(conf.getDirValueFactory())
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

	Comparator<Pair<WritableComparable, BucketID>> comp = new Comparator<Pair<WritableComparable, BucketID>>() {
		@Override
		public int compare(Pair<WritableComparable, BucketID> o1, Pair<WritableComparable, BucketID> o2) {
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
	public abstract Pair<WritableComparable, BucketID> floorOffset(WritableComparable curKey,
			WritableComparable curCode) throws IOException;

	public abstract Pair<WritableComparable, BucketID> cellOffset(WritableComparable curKey,
			WritableComparable curCode) throws IOException;

	@Override
	public SSTableMeta getMeta() {
		return meta;
	}

	@Override
	public Iterator<WritableComparable> keySetIter() {
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

	public Configuration getConf() {
		return conf;
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
