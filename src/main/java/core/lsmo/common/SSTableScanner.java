package core.lsmo.common;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map.Entry;

import core.lsmo.octree.IOctreeIterator;
import core.lsmt.ISSTableReader;
import core.lsmt.WritableComparable;
import util.Pair;

public class SSTableScanner implements Iterator<Entry<WritableComparable, IOctreeIterator>> {
	ISSTableReader reader;
	Iterator<WritableComparable> iter;

	public SSTableScanner(ISSTableReader reader) {
		this.reader = reader;
		iter = reader.keySetIter();
	}

	@Override
	public boolean hasNext() {
		return iter.hasNext();
	}

	@Override
	public Entry<WritableComparable, IOctreeIterator> next() {
		WritableComparable entry = iter.next();
		Pair<WritableComparable, IOctreeIterator> ret;
		try {
			ret = new Pair<WritableComparable, IOctreeIterator>(entry,
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
