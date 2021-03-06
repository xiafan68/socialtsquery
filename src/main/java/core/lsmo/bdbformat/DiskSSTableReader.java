package core.lsmo.bdbformat;

import java.io.IOException;

import core.commom.Encoding;
import core.lsmo.octree.DiskOctreeScanner;
import core.lsmo.octree.IOctreeIterator;
import core.lsmo.octree.OctreePostingListIter;
import core.lsmt.IMemTable.SSTableMeta;
import core.lsmt.WritableComparableKey;
import core.lsmt.WritableComparableKey.WritableComparableKeyFactory;
import core.lsmt.bdbindex.BucketBasedSSTableReader;
import core.lsmt.LSMTInvertedIndex;

/**
 * This class provides interfaces to locate a posting list given the keyword,
 * locate the position given the octree code and the interface to read the
 * bucket.
 * 
 * @author xiafan
 *
 */
public class DiskSSTableReader extends BucketBasedSSTableReader {
	private static enum EncodingFactory implements WritableComparableKeyFactory {
		INSTANCE;
		@Override
		public WritableComparableKey createIndexKey() {
			return new Encoding();
		}
	}

	public DiskSSTableReader(LSMTInvertedIndex index, SSTableMeta meta) {
		super(index, meta, EncodingFactory.INSTANCE);
	}

	public IOctreeIterator getPostingListScanner(WritableComparableKey key) {
		return new DiskOctreeScanner(dirMap.get(key), this);
	}

	public IOctreeIterator getPostingListIter(WritableComparableKey key, int start, int end) throws IOException {
		IOctreeIterator iter = new OctreePostingListIter(dirMap.get(key), this, start, end);
		iter.open();
		return iter;
	}
}
