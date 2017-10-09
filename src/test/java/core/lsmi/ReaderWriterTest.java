package core.lsmi;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.collections.map.DefaultedMap;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.PropertyConfigurator;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import common.MidSegment;
import common.TestDataGenerator;
import common.TestDataGeneratorBuilder;
import core.commom.WritableComparable;
import core.lsmt.IMemTable;
import core.lsmt.IMemTable.SSTableMeta;
import core.lsmt.LSMTInvertedIndex;
import core.lsmt.postinglist.IPostingListIterator;
import core.lsmt.postinglist.ISSTableReader;
import core.lsmt.postinglist.ISSTableWriter;
import util.Configuration;
import util.Pair;

public class ReaderWriterTest {
	private static final Logger logger = LoggerFactory.getLogger(ReaderWriterTest.class);
	LSMTInvertedIndex index;
	Configuration conf;

	@Before
	public void setupIndex() throws IOException {
		PropertyConfigurator.configure("conf/log4j-server.properties");
		conf = new Configuration();
		conf.load("conf/index.conf_template");
		conf.getIndexDir().mkdirs();
		conf.getTmpDir().mkdirs();
		index = new LSMTInvertedIndex(conf);
	}

	@After
	public void cleanUp() throws IOException {
		FileUtils.deleteDirectory(conf.getIndexDir());
	}

	@Test
	public void test() throws IOException {
		DefaultedMap termCounts = new DefaultedMap(0);

		TestDataGenerator gen = TestDataGeneratorBuilder.create().setMaxMidNum(10000).build();
		SSTableMeta meta = new SSTableMeta(0, 0);
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
		}

		List<IMemTable> tables = new ArrayList<IMemTable>();
		tables.add(tree);
		ISSTableWriter writer = SortedListBasedLSMTFactory.INSTANCE.newSSTableWriterForFlushing(tables, conf);
		writer.open(conf.getIndexDir());
		writer.write();
		writer.close();

		ISSTableReader reader = SortedListBasedLSMTFactory.INSTANCE.newSSTableReader(index, meta);
		reader.init();

		for (Object obj : termCounts.entrySet()) {
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

	public static void readerVerify(ISSTableReader reader, Configuration conf, int level) throws IOException {
		int expect = (conf.getFlushLimit() + 1) * (1 << level);
		int size = 0;
		Iterator<WritableComparable> iter = reader.keySetIter();
		while (iter.hasNext()) {
			WritableComparable key = iter.next();
			System.out.println("scanning postinglist of " + key);
			IPostingListIterator scanner = reader.getPostingListScanner(key);
			Pair<Integer, List<MidSegment>> cur = null;
			while (scanner.hasNext()) {
				cur = scanner.next();
				size += cur.getValue().size();
			}
			System.out.println("expect size:" + expect + " cursize size:" + size);

		}
		if (expect != size)
			System.err.println("expect size:" + expect + " total size:" + size);
		Assert.assertEquals(expect, size);
	}
}
