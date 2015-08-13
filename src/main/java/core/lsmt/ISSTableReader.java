package core.lsmt;

import java.io.IOException;
import java.util.Iterator;

import core.lsmo.octree.IOctreeIterator;
import core.lsmt.IMemTable.SSTableMeta;

/**
 * This class provides interfaces to locate a posting list given the keyword,
 * locate the position given the octree code.
 * 
 * @author xiafan
 *
 */
public abstract class ISSTableReader {
	protected LSMOInvertedIndex index;
	protected SSTableMeta meta;

	public ISSTableReader(LSMOInvertedIndex index, SSTableMeta meta) {
		this.index = index;
		this.meta = meta;
	}

	/**
	 * whether this reader has been initialized
	 * 
	 * @return
	 */
	public boolean isInited() {
		return true;
	}

	public void init() throws IOException {

	}

	public abstract Iterator<Integer> keySetIter();

	public abstract IOctreeIterator getPostingListScanner(int key) throws IOException;

	public abstract IOctreeIterator getPostingListIter(int key, int start, int end) throws IOException;

	public abstract void close() throws IOException;
}
