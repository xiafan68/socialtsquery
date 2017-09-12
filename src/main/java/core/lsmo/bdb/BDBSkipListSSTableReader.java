package core.lsmo.bdb;

import java.io.IOException;

import core.lsmo.common.InternDiskOctreeScanner;
import core.lsmt.IMemTable.SSTableMeta;
import core.lsmt.IPostingListIterator;
import core.lsmt.LSMTInvertedIndex;
import core.lsmt.WritableComparable;
import core.lsmt.WritableComparable.WritableComparableKeyFactory;
import core.lsmt.bdbindex.BucketBasedBDBSSTableReader;

public class BDBSkipListSSTableReader extends BucketBasedBDBSSTableReader {

	public BDBSkipListSSTableReader(LSMTInvertedIndex index, SSTableMeta meta,
			WritableComparableKeyFactory keyFactory) {
		super(index, meta, keyFactory);
	}

	@Override
	public IPostingListIterator getPostingListScanner(WritableComparable key) throws IOException {
		return new InternDiskOctreeScanner(getDirEntry(key), this);
	}

	@Override
	public IPostingListIterator getPostingListIter(WritableComparable key, int start, int end) throws IOException {
		return new BDBSkipListPostingListIter(getDirEntry(key), this, start, end);
	}

}
