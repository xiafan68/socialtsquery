package Util;

import java.util.Map.Entry;

public class Pair<K, V> implements Entry<K, V> {
	K key;
	V value;

	public Pair(K key, V value) {
		super();
		this.key = key;
		this.value = value;
	}

	@Override
	public K getKey() {
		return key;
	}

	@Override
	public V getValue() {
		return value;
	}

	@Override
	public V setValue(V value) {
		V pre = this.value;
		this.value = value;
		return pre;
	}

}
