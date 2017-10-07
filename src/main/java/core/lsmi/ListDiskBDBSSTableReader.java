package core.lsmi;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import common.MidSegment;
import core.commom.BDBBTreeBuilder;
import core.commom.BDBBtree;
import core.commom.IndexFileUtils;
import core.commom.WritableComparableKey;
import core.commom.WritableComparableKey.WritableComparableFactory;
import core.io.Block;
import core.io.Bucket;
import core.io.Bucket.BucketID;
import core.lsmo.bdbformat.OctreeSSTableWriter;
import core.io.SeekableDirectIO;
import core.lsmt.DirEntry;
import core.lsmt.IMemTable.SSTableMeta;
import core.lsmt.LSMTInvertedIndex;
import core.lsmt.postinglist.IPostingListIterator;
import core.lsmt.postinglist.PostingListMeta;
import util.Pair;
import util.Profile;
import util.ProfileField;

public class ListDiskBDBSSTableReader extends IBucketBasedSSTableReader {

	protected SeekableDirectIO dataInput;
	protected BDBBtree dirMap = null;
	protected AtomicBoolean init = new AtomicBoolean(false);

	protected LSMTInvertedIndex index;
	protected SSTableMeta meta;
	WritableComparableFactory keyFactory;

	public ListDiskBDBSSTableReader(LSMTInvertedIndex index, SSTableMeta meta, WritableComparableFactory keyFactory) {
		this.index = index;
		this.meta = meta;
		this.keyFactory = keyFactory;
	}

	public boolean isInited() {
		return true;
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
					dataInput = SeekableDirectIO.create(OctreeSSTableWriter.dataFile(dataDir, meta), "r");
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

	public static class SegListKey implements WritableComparableKey {
		int top;
		int start;
		long mid;

		public SegListKey() {

		}

		public SegListKey(int top, int start, long mid) {
			this.top = top;
			this.start = start;
			this.mid = mid;
		}

		@Override
		public int compareTo(WritableComparableKey o) {
			SegListKey obj = (SegListKey) o;
			int ret = Integer.compare(top, obj.top);
			if (ret == 0) {
				ret = Integer.compare(start, obj.start);
				if (ret == 0) {
					ret = Long.compare(mid, obj.mid);
				}
			}
			return ret;
		}

		@Override
		public void write(DataOutput output) throws IOException {
			output.writeInt(top);
			output.writeInt(start);
			output.writeLong(mid);
		}

		@Override
		public void read(DataInput input) throws IOException {
			top = input.readInt();
			start = input.readInt();
			mid = input.readLong();
		}
	}

	public static enum SegListKeyFactory implements WritableComparableFactory {
		INSTANCE;
		@Override
		public WritableComparableKey create() {
			return new SegListKey();
		}

	}

	public ListDiskBDBSSTableReader(LSMTInvertedIndex index, SSTableMeta meta) {
		super(index, meta, SegListKeyFactory.INSTANCE);
	}

	@Override
	public IPostingListIterator getPostingListScanner(WritableComparableKey key) throws IOException {
		return new ListDiskPostingListIterator(dirMap.get(key), 0, Integer.MAX_VALUE);
	}

	@Override
	public IPostingListIterator getPostingListIter(WritableComparableKey key, int start, int end) throws IOException {
		return new ListDiskPostingListIterator(dirMap.get(key), start, end);
	}

	class ListDiskPostingListIterator implements IPostingListIterator {
		DirEntry dir;

		BucketID nextBuckID;
		int nextBlockID;

		Bucket curBuck = null;
		SubList curList = new SubList(0);
		int curIdx = Integer.MAX_VALUE;

		MidSegment cur = null;
		int start;
		int end;

		public ListDiskPostingListIterator(DirEntry entry, int start, int end) {
			if (entry != null && entry.curKey != null && entry.minTime <= end && entry.maxTime >= start) {
				this.dir = entry;
				this.start = start;
				this.end = end;
				nextBuckID = new BucketID(entry.startBucketID);
			}
		}

		@Override
		public PostingListMeta getMeta() {
			return dir;
		}

		@Override
		public void open() throws IOException {

		}

		private void advance() throws IOException {
			while (cur == null && (curIdx < curList.size() || nextBuckID.compareTo(dir.endBucketID) <= 0)) {
				if (curIdx >= curList.size()) {
					loadSubList();
				}
				cur = curList.get(curIdx++);
				if (cur.getStart() <= end && cur.getEndTime() >= start)
					break;
				else {
					cur = null;
				}
			}
		}

		private void loadSubList() throws IOException {
			if (curBuck == null) {
				curBuck = new Bucket(0);
				nextBlockID = getBucket(nextBuckID, curBuck);
			} else if (nextBuckID.offset >= curBuck.octNum()) {
				nextBuckID.blockID = nextBlockID;
				nextBuckID.offset = 0;
				nextBlockID = getBucket(nextBuckID, curBuck);
			}
			curList.init();
			curIdx = 0;
			curList.read(new DataInputStream(new ByteArrayInputStream(curBuck.getOctree(nextBuckID.offset++))));
		}

		@Override
		public boolean hasNext() throws IOException {
			if (dir == null)
				return false;
			if (cur == null)
				advance();
			return cur != null;
		}

		@Override
		public Pair<Integer, List<MidSegment>> next() throws IOException {
			if (cur == null)
				advance();
			Pair<Integer, List<MidSegment>> ret = new Pair<Integer, List<MidSegment>>(cur.getPoint().getZ(),
					Arrays.asList(cur));
			cur = null;
			return ret;
		}

		@Override
		public void skipTo(WritableComparableKey key) throws IOException {
			// TODO Auto-generated method stub

		}

		@Override
		public void close() throws IOException {
		}

	}
}
