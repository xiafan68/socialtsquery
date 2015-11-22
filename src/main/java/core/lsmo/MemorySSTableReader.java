package core.lsmo;

import java.io.IOException;
import java.util.Iterator;

import core.lsmo.octree.IOctreeIterator;
import core.lsmo.octree.MemoryOctree;
import core.lsmo.octree.MemoryOctreeIterator;
import core.lsmt.IMemTable.SSTableMeta;
import core.lsmt.ISSTableReader;
import core.lsmt.ISSTableWriter.DirEntry;
import core.lsmt.WritableComparableKey;

public class MemorySSTableReader implements ISSTableReader {
	OctreeMemTable table;
	SSTableMeta meta;

	public MemorySSTableReader(OctreeMemTable table, SSTableMeta meta) {
		this.meta = meta;
		this.table = table;
	}

	@Override
	public IOctreeIterator getPostingListScanner(WritableComparableKey key) {
		return new MemoryOctreeIterator(table.get(key));
	}

	@Override
	public IOctreeIterator getPostingListIter(WritableComparableKey key, int start, int end) {
		MemoryOctree tree = table.get(key);
		return new MemoryOctreeIterator(tree, start, end);
	}

	@Override
	public Iterator<WritableComparableKey> keySetIter() {
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
	public DirEntry getDirEntry(WritableComparableKey key) {
		return null;
	}
}
