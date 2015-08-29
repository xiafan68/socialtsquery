package core.lsmo;

import java.io.IOException;
import java.util.Iterator;

import core.lsmo.octree.IOctreeIterator;
import core.lsmo.octree.MemoryOctree;
import core.lsmo.octree.MemoryOctreeIterator;
import core.lsmt.IMemTable.SSTableMeta;
import core.lsmt.ISSTableReader;
import core.lsmt.LSMTInvertedIndex;

public class MemorySSTableReader extends ISSTableReader {
	OctreeMemTable table;

	public MemorySSTableReader(LSMTInvertedIndex index, OctreeMemTable table, SSTableMeta meta) {
		super(index, meta);
		this.table = table;
	}

	@Override
	public IOctreeIterator getPostingListScanner(int key) {
		return new MemoryOctreeIterator(table.get(key));
	}

	@Override
	public IOctreeIterator getPostingListIter(int key, int start, int end) {
		MemoryOctree tree = table.get(key);
		return new MemoryOctreeIterator(tree, start, end);
	}

	@Override
	public Iterator<Integer> keySetIter() {
		return table.keySet().iterator();
	}

	@Override
	public void close() throws IOException {
	}
}
