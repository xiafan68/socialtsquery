package core.lsmo.internformat;

import java.io.File;
import java.io.IOException;
import java.util.Comparator;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.PropertyConfigurator;

import core.commom.BDBBTreeBuilder;
import core.commom.BDBBtree;
import core.commom.IndexFileUtils;
import core.commom.WritableComparableKey;
import core.commom.WritableComparableKey.WritableComparableFactory;
import core.io.Block;
import core.io.Block.BLOCKTYPE;
import core.io.Bucket;
import core.io.Bucket.BucketID;
import core.io.SeekableDirectIO;
import core.lsmt.DirEntry;
import core.lsmt.IBucketBasedSSTableReader;
import core.lsmt.IMemTable.SSTableMeta;
import core.lsmt.LSMTInvertedIndex;
import core.lsmt.postinglist.IPostingListIterator;
import util.Configuration;
import util.Pair;
import util.Profile;
import util.ProfileField;

public class BlockBasedSSTableReader implements IBucketBasedSSTableReader {
	protected SeekableDirectIO dataInput;
	protected SeekableDirectIO markInput;

	protected BDBBtree dirMap = null;

	protected AtomicBoolean init = new AtomicBoolean(false);

	protected LSMTInvertedIndex index;
	protected SSTableMeta meta;
	WritableComparableFactory valueFactory;
	WritableComparableFactory keyFactory;

	public BlockBasedSSTableReader(LSMTInvertedIndex index, SSTableMeta meta) {
		this.index = index;
		this.meta = meta;
		this.valueFactory = index.getConf().getIndexValueFactory();
		this.keyFactory = index.getConf().getDirKeyFactory();
	}

	public boolean isInited() {
		return true;
	}

	public DirEntry getDirEntry(WritableComparableKey key) throws IOException {
		DirEntry ret = (DirEntry) dirMap.get(key);
		ret.curKey = key;
		return ret;
	}

	public void init() throws IOException {
		if (!init.get()) {
			synchronized (this) {
				if (!init.get()) {
					File dataDir = index.getConf().getIndexDir();
					dataInput = SeekableDirectIO.create(InternOctreeSSTableWriter.dataFile(dataDir, meta), "r");
					markInput = SeekableDirectIO.create(InternOctreeSSTableWriter.markFile(dataDir, meta), "r");
					loadDirMeta();
				}
				init.set(true);
			}
		}
	}

	private void loadDirMeta() throws IOException {
		dirMap = BDBBTreeBuilder.create().setDir(IndexFileUtils.dirMetaFile(index.getConf().getIndexDir(), meta))
				.setKeyFactory(index.getConf().getDirKeyFactory()).setValueFactory(index.getConf().getDirValueFactory())
				.setAllowDuplicates(false).setReadOnly(true).build();
		dirMap.open();
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

	Comparator<Pair<WritableComparableKey, BucketID>> comp = new Comparator<Pair<WritableComparableKey, BucketID>>() {
		@Override
		public int compare(Pair<WritableComparableKey, BucketID> o1, Pair<WritableComparableKey, BucketID> o2) {
			return o1.getKey().compareTo(o2.getKey());
		}

	};

	public WritableComparableFactory getFactory() {
		return valueFactory;
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
	protected void finalize() throws Throwable {
		close();
	};

	@Override
	public void close() throws IOException {
		if (dataInput != null) {
			dataInput.close();
			dataInput = null;
			markInput.close();
			dirMap.close();
		}
	}

	@Override
	public IPostingListIterator getPostingListScanner(WritableComparableKey key) throws IOException {
		return new InternDiskOctreeScanner(getDirEntry(key), this);
	}

	@Override
	public IPostingListIterator getPostingListIter(WritableComparableKey key, int start, int end) throws IOException {
		return new InternPostingListIter(getDirEntry(key), this, start, end);
	}

	public static void main(String[] args) throws IOException {
		PropertyConfigurator.configure("conf/log4j-server.properties");
		Configuration conf = new Configuration();
		conf.load("conf/index_twitter_intern.conf");
		LSMTInvertedIndex index = new LSMTInvertedIndex(conf);
		BlockBasedSSTableReader reader = new BlockBasedSSTableReader(index, new SSTableMeta(255, 7));
		reader.init();
		Block block = new Block(BLOCKTYPE.DATA_BLOCK, 0);
		for (int i = 516000; i < 527300; i++) {
			block.setBlockIdx(i);
			reader.getBlockFromDataFile(block);
			// System.out.print(i + "");
			if (block.isDataBlock()) {
				// System.out.println(" is data");
			} else {
				System.out.println(i + " is meta");
			}
		}
		reader.close();

	}

	@Override
	public int getBucket(BucketID id, Bucket bucket) throws IOException {
		return 0;
	}

	@Override
	public Pair<WritableComparableKey, BucketID> cellOffset(WritableComparableKey curKey, WritableComparableKey curCode)
			throws IOException {
		return null;
	}

	@Override
	public Pair<WritableComparableKey, BucketID> floorOffset(WritableComparableKey curKey,
			WritableComparableKey curCode) throws IOException {
		return null;
	}
}