package core.lsmo;

import java.io.IOException;

import core.commom.Encoding;
import core.lsmo.octree.DiskOctreeIterator;
import core.lsmo.octree.IOctreeIterator;
import core.lsmo.octree.OctreePostingListIter;
import core.lsmt.BucketBasedSSTableReader;
import core.lsmt.IMemTable.SSTableMeta;
import core.lsmt.IndexKey;
import core.lsmt.IndexKey.IndexKeyFactory;
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
	private static enum EncodingFactory implements IndexKeyFactory {
		INSTANCE;
		@Override
		public IndexKey createIndexKey() {
			return new Encoding();
		}
	}

	public DiskSSTableReader(LSMTInvertedIndex index, SSTableMeta meta) {
		super(index, meta, EncodingFactory.INSTANCE);
	}

	public IOctreeIterator getPostingListScanner(int key) {
		return new DiskOctreeIterator(dirMap.get(key), this);
	}

	public IOctreeIterator getPostingListIter(int key, int start, int end)
			throws IOException {
		IOctreeIterator iter = new OctreePostingListIter(dirMap.get(key), this,
				start, end);
		iter.open();
		return iter;
	}
}
