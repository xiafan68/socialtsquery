package core.lsmo.internformat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
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
import core.lsmt.IMemTable.SSTableMeta;
import core.lsmt.postinglist.IPostingListIterator;
import core.lsmt.postinglist.ISSTableReader;
import core.lsmt.postinglist.ISSTableWriter;

public class InternFormatReaderWriterTest extends InternFormatCommon {

	@Test
	public void test() throws IOException {
		DefaultedMap termCounts = new DefaultedMap(0);

		TestDataGenerator gen = TestDataGeneratorBuilder.create().setMaxTerm(100).setMaxMidNum(1000).setMaxSegNum(10000)
				.build();
		SSTableMeta meta = new SSTableMeta(0, 0);
		genDiskSSTable(meta, gen, termCounts, 40000);
		ISSTableReader reader = OctreeInternFormatLSMTFactory.INSTANCE.newSSTableReader(index, meta);
		reader.init();

		int dataNum = 0;
		for (Object obj : termCounts.entrySet()) {
			if (dataNum++ % 100 == 0) {
				logger.info("validated " + dataNum + " terms");
			}
			Entry<String, Integer> entry = (Entry<String, Integer>) obj;
			WritableComparable key = new WritableComparable.StringKey(entry.getKey());
			Assert.assertEquals(entry.getValue().longValue(), reader.getDirEntry(key).size);
			IPostingListIterator scanner = reader.getPostingListScanner(key);
			int count = 0;
			Set<MidSegment> segs = new HashSet<MidSegment>();
			// Set<MidSegment> posting = inverted.get(key.toString());
			while (scanner.hasNext()) {
				List<MidSegment> newSegs = scanner.next().getValue();
				count += newSegs.size();
			}
			Assert.assertEquals(entry.getValue().longValue(), count);
		}
	}

	@Test
	public void compact() throws IOException {
		DefaultedMap termCounts = new DefaultedMap(0);
		TestDataGenerator gen = TestDataGeneratorBuilder.create().setMaxTerm(200).setMaxMidNum(2000).setMaxSegNum(4000)
				.build();

		SSTableMeta meta = new SSTableMeta(0, 0);
		genDiskSSTable(meta, gen, termCounts, 500000);
		ISSTableReader reader = OctreeInternFormatLSMTFactory.INSTANCE.newSSTableReader(index, meta);
		reader.init();

		SSTableMeta meta1 = new SSTableMeta(1, 0);
		genDiskSSTable(meta1, gen, termCounts, 500000);
		ISSTableReader reader1 = OctreeInternFormatLSMTFactory.INSTANCE.newSSTableReader(index, meta1);
		reader1.init();

		List<ISSTableReader> readers = new ArrayList<>();
		readers.add(reader);
		readers.add(reader1);

		SSTableMeta meta2 = new SSTableMeta(0, 1);
		ISSTableWriter writer = OctreeInternFormatLSMTFactory.INSTANCE.newSSTableWriterForCompaction(meta2, readers,
				conf);
		writer.open(conf.getIndexDir());
		writer.write();
		writer.close();

		ISSTableReader reader2 = OctreeInternFormatLSMTFactory.INSTANCE.newSSTableReader(index, meta2);
		reader2.init();
		int dataNum = 0;
		for (Object obj : termCounts.entrySet()) {
			if (dataNum++ % 100 == 0) {
				logger.info("validated " + dataNum + " terms");
			}
			Entry<String, Integer> entry = (Entry<String, Integer>) obj;
			WritableComparable key = new WritableComparable.StringKey(entry.getKey());
			Assert.assertEquals(entry.getValue().longValue(), reader2.getDirEntry(key).size);
			IPostingListIterator scanner = reader2.getPostingListScanner(key);
			int count = 0;
			// Set<MidSegment> segs = new HashSet<MidSegment>();
			// Set<MidSegment> posting = inverted.get(key.toString());
			while (scanner.hasNext()) {
				List<MidSegment> newSegs = scanner.next().getValue();
				count += newSegs.size();
			}
			Assert.assertEquals(entry.getValue().longValue(), count);
		}
	}
}
