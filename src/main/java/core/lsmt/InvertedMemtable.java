package core.lsmt;

import java.io.File;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;

import core.lsmo.octree.MemoryOctree;

public abstract class InvertedMemtable<VType> extends ConcurrentSkipListMap<WritableComparableKey, VType>
		implements IMemTable<VType> {
	private ConcurrentHashMap<WritableComparableKey, AtomicInteger> wordFreq = new ConcurrentHashMap<WritableComparableKey, AtomicInteger>();

	protected void hitWord(WritableComparableKey word) {
		if (!wordFreq.containsKey(word)) {
			wordFreq.putIfAbsent(word, new AtomicInteger(0));
		}
		wordFreq.get(word).incrementAndGet();
	}

	@Override
	public void writeStats(File dir) {

	}
}
