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

	public void setKey(K key) {
		this.key = key;
	}

	@Override
	public V setValue(V value) {
		V pre = this.value;
		this.value = value;
		return pre;
	}

	@Override
	public boolean equals(Object other) {
		Pair<K, V> oObj = (Pair<K, V>) other;
		return key.equals(oObj.key) && value.equals(oObj.value);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "Pair [key=" + key + ", value=" + value + "]";
	}
}
