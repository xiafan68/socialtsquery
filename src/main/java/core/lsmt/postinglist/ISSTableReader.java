package core.lsmt.postinglist;

import java.io.IOException;
import java.util.Iterator;

import core.commom.WritableComparable;
import core.lsmt.DirEntry;
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

	public DirEntry getDirEntry(WritableComparable key) throws IOException;

	/**
	 * whether this reader has been initialized
	 * 
	 * @return
	 */
	public boolean isInited();

	public void init() throws IOException;

	public abstract Iterator<WritableComparable> keySetIter();

	public abstract IPostingListIterator getPostingListScanner(WritableComparable key) throws IOException;

	public abstract IPostingListIterator getPostingListIter(WritableComparable key, int start, int end)
			throws IOException;

	public abstract void close() throws IOException;
}
