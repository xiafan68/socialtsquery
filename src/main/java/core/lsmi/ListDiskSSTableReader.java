package core.lsmi;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import Util.Pair;
import common.MidSegment;
import core.io.Bucket;
import core.io.Bucket.BucketID;
import core.lsmt.BucketBasedSSTableReader;
import core.lsmt.IMemTable.SSTableMeta;
import core.lsmt.ISSTableWriter.DirEntry;
import core.lsmt.IPostingListIterator;
import core.lsmt.IndexKey;
import core.lsmt.IndexKey.IndexKeyFactory;
import core.lsmt.LSMTInvertedIndex;
import core.lsmt.PostingListMeta;

public class ListDiskSSTableReader extends BucketBasedSSTableReader {

	public static class IntegerKey implements IndexKey {
		int val;

		public IntegerKey() {

		}

		public IntegerKey(int val) {
			this.val = val;
		}

		@Override
		public int compareTo(IndexKey o) {
			IntegerKey obj = (IntegerKey) o;
			return Integer.compare(val, obj.val);
		}

		@Override
		public void write(DataOutput output) throws IOException {
			output.writeInt(val);
		}

		@Override
		public void read(DataInput input) throws IOException {
			val = input.readInt();
		}
	}

	public static enum IntegerKeyFactory implements IndexKeyFactory {
		INSTANCE;
		@Override
		public IndexKey createIndexKey() {
			return new IntegerKey();
		}

	}

	public ListDiskSSTableReader(LSMTInvertedIndex index, SSTableMeta meta) {
		super(index, meta, IntegerKeyFactory.INSTANCE);
	}

	@Override
	public IPostingListIterator getPostingListScanner(int key)
			throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public IPostingListIterator getPostingListIter(int key, int start, int end)
			throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	class ListDiskPostingListIterator implements IPostingListIterator {
		Bucket curBuck = null;
		BucketID nextBuckID;
		int nextBlockID;
		DataInputStream dis = null;
		DirEntry dir;
		int totalCount = 0;
		int curBuckNum = 0;
		int curBuckCount = 0;
		MidSegment curSeg = null;
		int start;
		int end;

		public ListDiskPostingListIterator(DirEntry entry, int start, int end) {
			this.dir = entry;
			this.start = start;
			this.end = end;
		}

		@Override
		public PostingListMeta getMeta() {
			return dir;
		}

		@Override
		public void open() throws IOException {

		}

		private void advance() throws IOException {
			if (curSeg == null) {
				curSeg = new MidSegment();
				boolean readed = false;
				while (totalCount < dir.size) {
					if (curBuckCount >= curBuckNum) {
						if (curBuck == null
								|| nextBuckID.offset >= curBuck.octNum()) {
							nextBuckID.blockID = nextBlockID;
							nextBlockID = getBucket(nextBuckID, curBuck);
						}
						dis = new DataInputStream(new ByteArrayInputStream(
								curBuck.getOctree(nextBuckID.offset)));
						curBuckCount = 0;
						curBuckNum = dis.readInt();
					}

					while (curBuckCount++ < curBuckNum) {
						totalCount++;
						curSeg.read(dis);
						if (curSeg.getStart() <= end
								&& curSeg.getEndTime() >= start) {
							readed = true;
							break;
						}
					}
					if (readed)
						break;
				}
				if (!readed)
					curSeg = null;
			}
		}

		@Override
		public boolean hasNext() throws IOException {
			if (curSeg == null)
				advance();
			return curSeg != null;
		}

		@Override
		public Pair<Integer, List<MidSegment>> next() throws IOException {
			if (curSeg == null)
				advance();
			Pair<Integer, List<MidSegment>> ret = new Pair<Integer, List<MidSegment>>(
					curSeg.getPoint().getZ(), Arrays.asList(curSeg));
			curSeg = null;
			return ret;
		}

		@Override
		public void skipTo(IndexKey key) throws IOException {
			// TODO Auto-generated method stub

		}

		@Override
		public void close() throws IOException {
		}

	}
}
