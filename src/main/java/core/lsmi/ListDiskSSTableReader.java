package core.lsmi;

import java.io.IOException;
import java.util.Iterator;

import core.lsmt.IMemTable.SSTableMeta;
import core.lsmt.IPostingListIterator;
import core.lsmt.ISSTableReader;
import core.lsmt.LSMTInvertedIndex;

public class ListDiskSSTableReader extends ISSTableReader {

	public ListDiskSSTableReader(LSMTInvertedIndex index, SSTableMeta meta) {
		super(index, meta);
	}

	@Override
	public Iterator<Integer> keySetIter() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public IPostingListIterator getPostingListScanner(int key)
			throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public IPostingListIterator getPostingListIter(int key, int start, int end)
			throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub

	}

}
