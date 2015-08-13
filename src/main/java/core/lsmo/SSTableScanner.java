package core.lsmo;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map.Entry;

import Util.Pair;
import core.lsmo.octree.IOctreeIterator;
import core.lsmt.ISSTableReader;

public class SSTableScanner implements
		Iterator<Entry<Integer, IOctreeIterator>> {
	ISSTableReader reader;
	Iterator<Integer> iter;

	public SSTableScanner(ISSTableReader reader) {
		this.reader = reader;
		iter = reader.keySetIter();
	}

	@Override
	public boolean hasNext() {
		return iter.hasNext();
	}

	@Override
	public Entry<Integer, IOctreeIterator> next() {
		Integer entry = iter.next();
		Pair<Integer, IOctreeIterator> ret;
		try {
			ret = new Pair<Integer, IOctreeIterator>(entry,
					reader.getPostingListScanner(entry));
		} catch (IOException e) {
			e.printStackTrace();
			ret = null;
		}
		return ret;
	}

	@Override
	public void remove() {

	}

}
