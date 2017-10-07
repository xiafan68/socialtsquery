package core.lsmt;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;

import core.commom.WritableComparableKey;
import core.lsmt.postinglist.IPostingList;

/**
 * 最后还是决定使用一个独立的模块来统计每个单词的频率
 * 
 * @author xiafan
 *
 * @param <VType>
 */
public abstract class InvertedMemtable<pType extends IPostingList>
		extends ConcurrentSkipListMap<WritableComparableKey, pType> implements IMemTable<pType> {
	private ConcurrentHashMap<WritableComparableKey, AtomicInteger> wordFreq = new ConcurrentHashMap<WritableComparableKey, AtomicInteger>();
	protected SSTableMeta meta;
	// for the reason of multiple thread
	protected volatile boolean frezen = false;
	protected volatile int valueCount = 0;
	private AtomicInteger docSeq = new AtomicInteger(0);

	public InvertedMemtable(SSTableMeta meta) {
		this.meta = meta;
	}

	public int increDocSeq() {
		return docSeq.incrementAndGet();
	}

	protected void hitWord(WritableComparableKey word) {
		if (!wordFreq.containsKey(word)) {
			wordFreq.putIfAbsent(word, new AtomicInteger(0));
		}
		wordFreq.get(word).incrementAndGet();
	}

	public void freeze() {
		frezen = true;
	}

	public int size() {
		return valueCount;
	}

	public static File dirMetaFile(File dir, SSTableMeta meta) {
		return new File(dir, String.format("%d_%d.stats", meta.version, meta.level));
	}

	public Iterator<Entry<WritableComparableKey, pType>> iterator() {
		return entrySet().iterator();
	}

	@Override
	public pType getPostingList(WritableComparableKey key) {
		return get(key);
	}

	@Override
	public void writeStats(File dir) throws IOException {
		File statsFile = dirMetaFile(dir, meta);
		BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(statsFile));
		DataOutputStream dos = new DataOutputStream(bos);
		for (Entry<WritableComparableKey, AtomicInteger> entry : wordFreq.entrySet()) {
			entry.getKey().write(dos);
			dos.writeInt(entry.getValue().get());
		}
		dos.close();
		bos.close();
	}
}
