package core.lsmo;

import java.io.IOException;
import java.util.Iterator;

import core.commom.MemoryPostingListIterUtil;
import core.commom.WritableComparable;
import core.lsmo.octree.IOctreeIterator;
import core.lsmo.octree.MemoryOctree;
import core.lsmo.octree.MemoryOctreeIterator;
import core.lsmt.DirEntry;
import core.lsmt.IMemTable.SSTableMeta;
import core.lsmt.postinglist.IPostingListIterator;
import core.lsmt.postinglist.ISSTableReader;

public class MemorySSTableReader implements ISSTableReader {
	OctreeMemTable table;
	SSTableMeta meta;

	public MemorySSTableReader(OctreeMemTable table, SSTableMeta meta) {
		this.meta = meta;
		this.table = table;
	}

	@Override
	public IOctreeIterator getPostingListScanner(WritableComparable key) {
		return new MemoryOctreeIterator(table.get(key));
	}

	@Override
	public IPostingListIterator getPostingListIter(WritableComparable key, int start, int end) {
		MemoryOctree tree = table.get(key);
		MemoryOctreeIterator iter = new MemoryOctreeIterator(tree, start, end);
		return MemoryPostingListIterUtil.getPostingListIter(iter, start, end);
	}

	@Override
	public Iterator<WritableComparable> keySetIter() {
		return table.keySet().iterator();
	}

	@Override
	public void close() throws IOException {
	}

	@Override
	public SSTableMeta getMeta() {
		return meta;
	}

	@Override
	public boolean isInited() {
		return true;
	}

	@Override
	public void init() throws IOException {
	}

	@Override
	public DirEntry getDirEntry(WritableComparable key) {
		return null;
	}
}
