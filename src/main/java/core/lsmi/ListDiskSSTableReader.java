package core.lsmi;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import common.MidSegment;
import core.io.Bucket;
import core.io.Bucket.BucketID;
import core.lsmt.IMemTable.SSTableMeta;
import core.lsmt.ISSTableWriter.DirEntry;
import core.lsmt.IPostingListIterator;
import core.lsmt.WritableComparable;
import core.lsmt.WritableComparable.WritableComparableKeyFactory;
import core.lsmt.bdbindex.BucketBasedSSTableReader;
import util.Pair;
import core.lsmt.LSMTInvertedIndex;
import core.lsmt.PostingListMeta;

public class ListDiskSSTableReader extends BucketBasedSSTableReader {

	public static class SegListKey implements WritableComparable {
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
		public int compareTo(WritableComparable o) {
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

	public static enum SegListKeyFactory implements WritableComparableKeyFactory {
		INSTANCE;
		@Override
		public WritableComparable createIndexKey() {
			return new SegListKey();
		}

	}

	public ListDiskSSTableReader(LSMTInvertedIndex index, SSTableMeta meta) {
		super(index, meta, SegListKeyFactory.INSTANCE);
	}

	@Override
	public IPostingListIterator getPostingListScanner(WritableComparable key) throws IOException {
		return new ListDiskPostingListIterator(dirMap.get(key), 0, Integer.MAX_VALUE);
	}

	@Override
	public IPostingListIterator getPostingListIter(WritableComparable key, int start, int end) throws IOException {
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
			if (entry != null) {
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
		public void skipTo(WritableComparable key) throws IOException {
			// TODO Auto-generated method stub

		}

		@Override
		public void close() throws IOException {
		}

	}
}
