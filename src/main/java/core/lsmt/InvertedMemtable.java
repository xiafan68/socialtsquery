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

/**
 * 最后还是决定使用一个独立的模块来统计每个单词的频率
 * 
 * @author xiafan
 *
 * @param <VType>
 */
public abstract class InvertedMemtable<pType extends IPostingList>
		extends ConcurrentSkipListMap<WritableComparable, pType> implements IMemTable<pType> {
	private ConcurrentHashMap<WritableComparable, AtomicInteger> wordFreq = new ConcurrentHashMap<WritableComparable, AtomicInteger>();
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

	protected void hitWord(WritableComparable word) {
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

	public Iterator<Entry<WritableComparable, pType>> iterator() {
		return entrySet().iterator();
	}

	@Override
	public pType getPostingList(WritableComparable key) {
		return get(key);
	}

	@Override
	public void writeStats(File dir) throws IOException {
		File statsFile = dirMetaFile(dir, meta);
		BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(statsFile));
		DataOutputStream dos = new DataOutputStream(bos);
		for (Entry<WritableComparable, AtomicInteger> entry : wordFreq.entrySet()) {
			entry.getKey().write(dos);
			dos.writeInt(entry.getValue().get());
		}
		dos.close();
		bos.close();
	}
}
