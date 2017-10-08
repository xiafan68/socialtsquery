package core.lsmi;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import common.MidSegment;
import core.commom.WritableComparableKey;
import core.commom.WritableComparableKey.WritableComparableFactory;
import core.io.Bucket;
import core.io.Bucket.BucketID;
import core.lsmt.DirEntry;
import core.lsmt.IBucketBasedSSTableReader;
import core.lsmt.IMemTable.SSTableMeta;
import core.lsmt.LSMTInvertedIndex;
import core.lsmt.postinglist.IPostingListIterator;
import core.lsmt.postinglist.PostingListMeta;
import util.Pair;

public class ListDiskBDBSSTableReader extends IBucketBasedSSTableReader {
	public ListDiskBDBSSTableReader(LSMTInvertedIndex index, SSTableMeta meta, WritableComparableFactory keyFactory) {
		super(index, meta, keyFactory);
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
		super(index, meta);
	}

	@Override
	public IPostingListIterator getPostingListScanner(WritableComparableKey key) throws IOException {
		return new ListDiskPostingListIterator(getDirEntry(key), 0, Integer.MAX_VALUE);
	}

	@Override
	public IPostingListIterator getPostingListIter(WritableComparableKey key, int start, int end) throws IOException {
		return new ListDiskPostingListIterator(getDirEntry(key), start, end);
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

	@Override
	public void initIndex() throws IOException {
		// nothing to do
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

	@Override
	public void closeIndex() {
		// nothing to do
	}
}
