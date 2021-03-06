package core.lsmt;

import java.io.IOException;
import java.util.Iterator;

import core.lsmt.IMemTable.SSTableMeta;
import core.lsmt.ISSTableWriter.DirEntry;

/**
 * This class provides interfaces to locate a posting list given the keyword,
 * locate the position given the octree code.
 * 
 * @author xiafan
 *
 */
public interface ISSTableReader {
	public SSTableMeta getMeta();

	public DirEntry getDirEntry(WritableComparableKey key) throws IOException;

	/**
	 * whether this reader has been initialized
	 * 
	 * @return
	 */
	public boolean isInited();

	public void init() throws IOException;

	public abstract Iterator<WritableComparableKey> keySetIter();

	public abstract IPostingListIterator getPostingListScanner(WritableComparableKey key) throws IOException;

	public abstract IPostingListIterator getPostingListIter(WritableComparableKey key, int start, int end)
			throws IOException;

	public abstract void close() throws IOException;
}
