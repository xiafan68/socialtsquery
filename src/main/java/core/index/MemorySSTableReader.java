package core.index;

import java.io.IOException;
import java.util.Iterator;

import core.index.MemTable.SSTableMeta;
import core.index.octree.IOctreeIterator;
import core.index.octree.MemoryOctreeIterator;

public class MemorySSTableReader extends ISSTableReader {
	MemTable table;

	public MemorySSTableReader(LSMOInvertedIndex index, MemTable table, SSTableMeta meta) {
		super(index, meta);
		this.table = table;
	}

	@Override
	public IOctreeIterator getPostingListScanner(int key) {
		return new MemoryOctreeIterator(table.get(key));
	}

	@Override
	public IOctreeIterator getPostingListIter(int key, int start, int end) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Iterator<Integer> keySetIter() {
		return table.keySet().iterator();
	}

	@Override
	public void close() throws IOException {
	}

}
