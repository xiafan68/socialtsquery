package core.lsmt;

import java.io.IOException;
import java.util.Iterator;

import core.lsmt.IMemTable.SSTableMeta;

/**
 * This class provides interfaces to locate a posting list given the keyword,
 * locate the position given the octree code.
 * 
 * @author xiafan
 *
 */
public interface ISSTableReader {
	public SSTableMeta getMeta();

	/**
	 * whether this reader has been initialized
	 * 
	 * @return
	 */
	public boolean isInited();

	public void init() throws IOException;

	public abstract Iterator<Integer> keySetIter();

	public abstract IPostingListIterator getPostingListScanner(int key)
			throws IOException;

	public abstract IPostingListIterator getPostingListIter(int key, int start,
			int end) throws IOException;

	public abstract void close() throws IOException;
}
