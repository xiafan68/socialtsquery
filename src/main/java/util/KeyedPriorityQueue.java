package util;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * 小根堆
 * 
 * @author xiafan
 *
 * @param <K>
 * @param <V>
 */
public class KeyedPriorityQueue<K, V> {
	ArrayList<Pair<K, V>> data = new ArrayList<Pair<K, V>>();
	Map<K, Integer> keyIndex = new HashMap<K, Integer>();
	Comparator<V> comp = null;;

	public KeyedPriorityQueue(Comparator<V> comp) {
		this.comp = comp;
	}

	public KeyedPriorityQueue() {
	}

	public boolean isEmpty() {
		return keyIndex.isEmpty();
	}

	public int size() {
		return keyIndex.size();
	}

	public V poll() {
		Pair<K, V> ret = data.get(0);
		remove(ret.getKey());
		return ret.getValue();
	}

	public V first() {
		return data.get(0).getValue();
	}

	public void offer(K key, V value) {
		int idx = -1;
		if (keyIndex.containsKey(key)) {
			idx = keyIndex.get(key);
		} else {
			data.add(new Pair<K, V>(key, value));
			idx = data.size() - 1;
			keyIndex.put(key, idx);
		}
		buildHeapFromBottom(idx);
	}

	/**
	 * 从当前idx更新到根节点的整条路径
	 * 
	 * @param idx
	 */
	private void buildHeapFromBottom(int idx) {
		while (idx != 0) {
			int parent = (idx - 1) / 2;
			if ((comp != null && comp.compare(data.get(idx).getValue(), data.get(parent).getValue()) < 0)) {
				swap(parent, idx);
				idx = parent;
			} else {
				break;
			}
		}
	}

	/**
	 * 
	 * @param idx
	 */
	private void buildHeapFromTop(int idx) {
		int leftChild = idx * 2 + 1;
		int largeIndex = idx;
		if (leftChild < data.size()) {
			if ((comp != null && comp.compare(data.get(leftChild).getValue(), data.get(idx).getValue()) < 0)) {
				largeIndex = leftChild;
			}
			int rightChild = leftChild + 1;
			if (rightChild < data.size() && ((comp != null
					&& comp.compare(data.get(rightChild).getValue(), data.get(largeIndex).getValue()) < 0))) {
				largeIndex = rightChild;
			}
		}
		if (largeIndex != idx) {
			swap(idx, largeIndex);
			buildHeapFromTop(largeIndex);
		}
	}

	private void swap(int parent, int child) {
		Pair<K, V> tmp = data.get(child);
		data.set(child, data.get(parent));
		data.set(parent, tmp);
		keyIndex.put(tmp.getKey(), parent);
		keyIndex.put(data.get(child).getKey(), child);
	}

	public boolean contains(K key) {
		return keyIndex.containsKey(key);
	}

	public int getIndex(K key) {
		return keyIndex.get(key);
	}

	public void updateFromBottom(K key) {
		buildHeapFromBottom(getIndex(key));
	}

	public void updateFromTop(K key) {
		buildHeapFromTop(getIndex(key));
	}

	public V getValue(K key) {
		return data.get(keyIndex.get(key)).getValue();
	}

	public Iterator<Pair<K, V>> tailIterator() {
		return new Iterator<Pair<K, V>>() {
			@Override
			public boolean hasNext() {
				return !data.isEmpty();
			}

			@Override
			public Pair<K, V> next() {
				return data.get(data.size() - 1);
			}

			@Override
			public void remove() {
				keyIndex.remove(data.remove(data.size() - 1).getKey());
			}
		};
	}

	public void remove(K key) {
		if (keyIndex.containsKey(key)) {
			int idx = keyIndex.get(key);
			if (idx == data.size() - 1) {
				data.remove(idx);
			} else {
				Pair<K, V> pair = data.remove(data.size() - 1);
				keyIndex.put(pair.getKey(), idx);
				data.set(idx, pair);
				buildHeapFromTop(idx);
			}
			keyIndex.remove(key);
		}
	}

	public Iterator<V> iterator() {
		return new Iterator<V>() {
			Iterator<Pair<K, V>> iter = data.iterator();

			@Override
			public boolean hasNext() {
				return iter.hasNext();
			}

			@Override
			public V next() {
				return iter.next().getValue();
			}

			@Override
			public void remove() {

			}
		};
	}
}
