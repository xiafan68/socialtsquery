package core.lsmo.bdbformat;

import java.io.IOException;

import core.commom.Encoding;
import core.commom.WritableComparableKey;
import core.commom.WritableComparableKey.WritableComparableFactory;
import core.io.Bucket.BucketID;
import core.lsmo.octree.DiskOctreeScanner;
import core.lsmo.octree.IOctreeIterator;
import core.lsmo.octree.OctreePostingListIter;
import core.lsmt.IBucketBasedSSTableReader;
import core.lsmt.IMemTable.SSTableMeta;
import core.lsmt.LSMTInvertedIndex;
import util.Pair;

/**
 * This class provides interfaces to locate a posting list given the keyword,
 * locate the position given the octree code and the interface to read the
 * bucket.
 * 
 * @author xiafan
 *
 */
public class DiskSSTableBDBReader extends IBucketBasedSSTableReader {
	private static enum EncodingFactory implements WritableComparableFactory {
		INSTANCE;
		@Override
		public WritableComparableKey create() {
			return new Encoding();
		}
	}

	public DiskSSTableBDBReader(LSMTInvertedIndex index, SSTableMeta meta) {
		super(index, meta);
	}

	public IOctreeIterator getPostingListScanner(WritableComparableKey key) throws IOException {
		return new DiskOctreeScanner(getDirEntry(key), this);
	}

	public IOctreeIterator getPostingListIter(WritableComparableKey key, int start, int end) throws IOException {
		IOctreeIterator iter = new OctreePostingListIter(getDirEntry(key), this, start, end);
		iter.open();
		return iter;
	}

	@Override
	public void initIndex() throws IOException {
		// nothing to do
	}

	@Override
	public Pair<WritableComparableKey, BucketID> cellOffset(WritableComparableKey curKey, WritableComparableKey curCode)
			throws IOException {
		// nothing to do
		return null;
	}

	@Override
	public Pair<WritableComparableKey, BucketID> floorOffset(WritableComparableKey curKey,
			WritableComparableKey curCode) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void closeIndex() {
		// nothing to do
	}
}
