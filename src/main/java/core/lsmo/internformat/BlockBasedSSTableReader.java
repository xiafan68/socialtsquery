package core.lsmo.internformat;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Comparator;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.PropertyConfigurator;

import core.commom.BDBBtree;
import core.io.Block;
import core.io.Block.BLOCKTYPE;
import core.io.Bucket.BucketID;
import core.lsmo.bdbformat.OctreeSSTableWriter;
import core.lsmt.IMemTable.SSTableMeta;
import core.lsmt.IPostingListIterator;
import core.lsmt.ISSTableReader;
import core.lsmt.ISSTableWriter.DirEntry;
import core.lsmt.LSMTInvertedIndex;
import core.lsmt.WritableComparableKey;
import core.lsmt.WritableComparableKey.WritableComparableKeyFactory;
import core.lsmt.bdbindex.BDBBasedIndexHelper;
import util.Configuration;
import util.Pair;
import util.Profile;

public class BlockBasedSSTableReader implements ISSTableReader {
	protected RandomAccessFile dataInput;
	protected BDBBtree dirMap = null;

	protected AtomicBoolean init = new AtomicBoolean(false);

	protected LSMTInvertedIndex index;
	protected SSTableMeta meta;
	WritableComparableKeyFactory keyFactory;

	public BlockBasedSSTableReader(LSMTInvertedIndex index, SSTableMeta meta) {
		this.index = index;
		this.meta = meta;
		this.keyFactory = index.getConf().getIndexValueFactory();
	}

	public boolean isInited() {
		return true;
	}

	public DirEntry getDirEntry(WritableComparableKey key) throws IOException {
		return dirMap.get(key);
	}

	public void init() throws IOException {
		if (!init.get()) {
			synchronized (this) {
				if (!init.get()) {
					File dataDir = index.getConf().getIndexDir();
					dataInput = new RandomAccessFile(OctreeSSTableWriter.dataFile(dataDir, meta), "r");
					loadDirMeta();
				}
				init.set(true);
			}
		}
	}

	private void loadDirMeta() throws IOException {
		dirMap = new BDBBtree(BDBBasedIndexHelper.dirMetaFile(index.getConf().getIndexDir(), meta), index.getConf());
		dirMap.open(false, false);
	}

	public synchronized int getBlockFromDataFile(Block block) throws IOException {
		Profile.instance.start("block");
		try {
			dataInput.seek(block.getFileOffset());
			block.read(dataInput);
			return (int) (dataInput.getChannel().position() / Block.BLOCK_SIZE);
		} finally {
			Profile.instance.end("block");
		}
	}

	Comparator<Pair<WritableComparableKey, BucketID>> comp = new Comparator<Pair<WritableComparableKey, BucketID>>() {
		@Override
		public int compare(Pair<WritableComparableKey, BucketID> o1, Pair<WritableComparableKey, BucketID> o2) {
			return o1.getKey().compareTo(o2.getKey());
		}

	};

	public WritableComparableKeyFactory getFactory() {
		return keyFactory;
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
		dirMap.close();
	}

	@Override
	public IPostingListIterator getPostingListScanner(WritableComparableKey key) throws IOException {
		return new InternPostingListIter(dirMap.get(key), this, 0, Integer.MAX_VALUE);
	}

	@Override
	public IPostingListIterator getPostingListIter(WritableComparableKey key, int start, int end) throws IOException {
		return new InternPostingListIter(dirMap.get(key), this, start, end);
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
			//System.out.print(i + "");
			if (block.isDataBlock()) {
				//System.out.println(" is data");
			} else {
				System.out.println(i+" is meta");
			}
		}
		reader.close();

	}
}