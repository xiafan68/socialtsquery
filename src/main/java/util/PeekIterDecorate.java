package util;

import java.util.Iterator;

public class PeekIterDecorate {

	public static <T> PeekableClosableIterator<T> decorate(Iterator<T> iter) {
		return new PeekableClosableIterator<T>(iter);
	}

	public static class PeekableClosableIterator<T> implements Iterator<T> {
		T curEntry = null;
		Iterator<T> iter;

		public PeekableClosableIterator(Iterator<T> iter) {
			this.iter = iter;
		}

		@Override
		public boolean hasNext() {
			return curEntry != null || iter.hasNext();
		}

		public T peek() {
			if (curEntry == null)
				curEntry = iter.next();
			return curEntry;
		}

		@Override
		public T next() {
			T ret = peek();
			curEntry = null;
			return ret;
		}

		@Override
		public void remove() {

		}
	}
}
