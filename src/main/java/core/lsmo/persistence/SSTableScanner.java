package core.lsmo.persistence;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map.Entry;

import core.lsmo.octree.IOctreeIterator;
import core.lsmt.WritableComparableKey;
import core.lsmt.postinglist.ISSTableReader;
import util.Pair;

public class SSTableScanner implements Iterator<Entry<WritableComparableKey, IOctreeIterator>> {
	ISSTableReader reader;
	Iterator<WritableComparableKey> iter;

	public SSTableScanner(ISSTableReader reader) {
		this.reader = reader;
		iter = reader.keySetIter();
	}

	@Override
	public boolean hasNext() {
		return iter.hasNext();
	}

	@Override
	public Entry<WritableComparableKey, IOctreeIterator> next() {
		WritableComparableKey entry = iter.next();
		Pair<WritableComparableKey, IOctreeIterator> ret;
		try {
			ret = new Pair<WritableComparableKey, IOctreeIterator>(entry,
					(IOctreeIterator) reader.getPostingListScanner(entry));
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
