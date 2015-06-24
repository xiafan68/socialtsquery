package core.index;

import java.util.Iterator;
import java.util.Map.Entry;

import Util.Pair;
import core.index.SSTableWriter.DirEntry;
import core.index.octree.IOctreeIterator;

public class SSTableScanner implements
		Iterator<Entry<Integer, IOctreeIterator>> {
	SSTableReader reader;
	Iterator<Entry<Integer, DirEntry>> iter;

	public SSTableScanner(SSTableReader reader) {
		this.reader = reader;
		iter = reader.entries.entrySet().iterator();
	}

	@Override
	public boolean hasNext() {
		return iter.hasNext();
	}

	@Override
	public Entry<Integer, IOctreeIterator> next() {
		Entry<Integer, DirEntry> entry = iter.next();
		Pair<Integer, IOctreeIterator> ret = new Pair<Integer, IOctreeIterator>(
				entry.getKey(), reader.getPostingListScanner(entry.getKey()));
		return ret;
	}

	@Override
	public void remove() {

	}

}
