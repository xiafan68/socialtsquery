package Util;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.PriorityQueue;

import Util.PeekIterDecorate.PeekableClosableIterator;

public class GroupByKeyIterator<K, V> implements Iterator<Entry<K, List<V>>> {
	PriorityQueue<PeekableClosableIterator<Entry<K, V>>> queue;
	Comparator<K> comp;

	public GroupByKeyIterator(final Comparator<K> comp) {
		this.comp = comp;
		queue = new PriorityQueue<PeekableClosableIterator<Entry<K, V>>>(2,
				new Comparator<PeekableClosableIterator<Entry<K, V>>>() {
					@Override
					public int compare(
							PeekableClosableIterator<Entry<K, V>> o1,
							PeekableClosableIterator<Entry<K, V>> o2) {
						if (!o1.hasNext())
							return -1;
						if (!o2.hasNext()) {
							return 1;
						}
						Entry<K, V> e1 = o1.peek();
						Entry<K, V> e2 = o2.peek();
						return comp.compare(e1.getKey(), e2.getKey());
					}
				});
	}

	public GroupByKeyIterator(
			final List<PeekableClosableIterator<Entry<K, V>>> iters,
			final Comparator<K> comp) {
		this.comp = comp;
		queue = new PriorityQueue<PeekableClosableIterator<Entry<K, V>>>(
				iters.size(),
				new Comparator<PeekableClosableIterator<Entry<K, V>>>() {
					@Override
					public int compare(
							PeekableClosableIterator<Entry<K, V>> o1,
							PeekableClosableIterator<Entry<K, V>> o2) {
						if (!o1.hasNext())
							return -1;
						if (!o2.hasNext()) {
							return 1;
						}
						Entry<K, V> e1 = o1.peek();
						Entry<K, V> e2 = o2.peek();
						return comp.compare(e1.getKey(), e2.getKey());
					}
				});
		queue.addAll(iters);
	}

	public void add(PeekableClosableIterator<Entry<K, V>> iter) {
		queue.offer(iter);
	}

	public Entry<K, List<V>> next() {
		if (queue.isEmpty())
			return null;

		K key = queue.peek().peek().getKey();
		Entry<K, List<V>> ret = new Pair<K, List<V>>(key, new ArrayList<V>());
		while (!queue.isEmpty()) {
			Entry<K, V> entry = queue.peek().peek();
			K curKey = entry.getKey();
			if (comp.compare(key, curKey) == 0) {
				ret.getValue().add(entry.getValue());
				PeekableClosableIterator<Entry<K, V>> iter = queue.poll();
				iter.next();
				if (iter.hasNext()) {
					queue.offer(iter);
				}
			} else {
				break;
			}
		}
		return ret;
	}

	@Override
	public boolean hasNext() {
		return !queue.isEmpty();
	}

	@Override
	public void remove() {

	}
}
