package core.lsmi;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.collections.map.DefaultedMap;
import org.junit.Assert;
import org.junit.Test;

import common.MidSegment;
import common.TestDataGenerator;
import common.TestDataGeneratorBuilder;
import core.commom.WritableComparable;
import core.lsmt.IMemTable;
import core.lsmt.IMemTable.SSTableMeta;
import core.lsmt.postinglist.IPostingListIterator;
import core.lsmt.postinglist.ISSTableReader;
import core.lsmt.postinglist.ISSTableWriter;
import util.Pair;

public class LSMICompactTest extends LSMITestCommon {

	private void genDiskSSTable(SSTableMeta meta, TestDataGenerator gen, DefaultedMap termCounts, int sizeLimit)
			throws IOException {
		SortedListMemTable tree = new SortedListMemTable(index, meta);
		int dataNum = 0;
		while (gen.hasNext()) {
			if (dataNum++ % 100 == 0) {
				logger.info("inserted " + dataNum + " items");
			}
			Pair<Set<String>, MidSegment> data = gen.nextData();
			for (String term : data.getKey()) {
				termCounts.put(term, (Integer) termCounts.get(term) + 1);
				tree.insert(new WritableComparable.StringKey(term), data.getValue());
			}
			if (dataNum > sizeLimit)
				break;
		}

		@SuppressWarnings("rawtypes")
		List<IMemTable> tables = new ArrayList<IMemTable>();
		tables.add(tree);
		ISSTableWriter writer = SortedListBasedLSMTFactory.INSTANCE.newSSTableWriterForFlushing(tables, conf);
		writer.open(conf.getIndexDir());
		writer.write();
		writer.close();
	}

	@Test
	public void test() throws IOException {
		DefaultedMap termCounts = new DefaultedMap(0);

		TestDataGenerator gen = TestDataGeneratorBuilder.create().setMaxMidNum(20000).build();
		SSTableMeta meta1 = new SSTableMeta(0, 0);
		genDiskSSTable(meta1, gen, termCounts, 400000);

		SSTableMeta meta2 = new SSTableMeta(1, 0);
		genDiskSSTable(meta2, gen, termCounts, 400000);

		SSTableMeta meta3 = new SSTableMeta(0, 1);
		List<ISSTableReader> readers = new ArrayList<ISSTableReader>();
		ISSTableReader reader = SortedListBasedLSMTFactory.INSTANCE.newSSTableReader(index, meta1);
		reader.init();
		readers.add(reader);
		reader = SortedListBasedLSMTFactory.INSTANCE.newSSTableReader(index, meta2);
		reader.init();
		readers.add(reader);
		ISSTableWriter writer = SortedListBasedLSMTFactory.INSTANCE.newSSTableWriterForCompaction(meta3, readers, conf);
		writer.open(conf.getIndexDir());
		writer.write();
		writer.close();

		reader = SortedListBasedLSMTFactory.INSTANCE.newSSTableReader(index, meta3);
		reader.init();
		for (Object obj : termCounts.entrySet()) {
			@SuppressWarnings("unchecked")
			Entry<String, Integer> entry = (Entry<String, Integer>) obj;
			WritableComparable key = new WritableComparable.StringKey(entry.getKey());
			Assert.assertEquals(entry.getValue().longValue(), reader.getDirEntry(key).size);
			IPostingListIterator scanner = reader.getPostingListScanner(key);
			int count = 0;
			while (scanner.hasNext()) {
				count++;
				scanner.next();
			}
			Assert.assertEquals(entry.getValue().longValue(), count);
		}
	}
}
