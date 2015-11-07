package core.lsmi;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.log4j.PropertyConfigurator;
import org.junit.Assert;
import org.junit.Test;

import Util.Configuration;
import Util.Pair;

import common.MidSegment;

import core.lsmo.octree.OctreeNode;
import core.lsmo.octree.OctreeNode.CompressedSerializer;
import core.lsmt.IMemTable;
import core.lsmt.IMemTable.SSTableMeta;
import core.lsmt.IPostingListIterator;
import core.lsmt.ISSTableReader;
import core.lsmt.ISSTableWriter;
import core.lsmt.LSMTInvertedIndex;
import core.lsmt.WritableComparableKey;
import fanxia.file.DirLineReader;

public class ReaderWriterTest {
	@Test
	public void test() throws IOException {
		PropertyConfigurator.configure("conf/log4j-server.properties");
		Configuration conf = new Configuration();
		conf.load("conf/index.conf");

		LSMTInvertedIndex index = new LSMTInvertedIndex(conf);
		OctreeNode.HANDLER = CompressedSerializer.INSTANCE;
		// "/home/xiafan/dataset/twitter/twitter_segs"
		DirLineReader reader = new DirLineReader("/Users/xiafan/Documents/dataset/expr/twitter/twitter_segs");
		String line = null;
		SSTableMeta meta = new SSTableMeta(0, 0);
		SortedListMemTable tree = new SortedListMemTable(index, meta);
		while (null != (line = reader.readLine())) {
			MidSegment seg = new MidSegment();
			seg.parse(line);
			tree.insert(new WritableComparableKey.StringKey(
					Long.toString(Math.abs(Long.toString(seg.getMid()).hashCode()) % 10)), seg);
			if (tree.size() == conf.getFlushLimit() + 1) {
				break;
			}
		}
		reader.close();

		List<IMemTable> tables = new ArrayList<IMemTable>();
		tables.add(tree);
		ISSTableWriter writer = SortedListBasedLSMTFactory.INSTANCE.newSSTableWriterForFlushing(tables, conf);
		writer.open(conf.getIndexDir());
		writer.write();
		writer.close();

		ISSTableReader treader = SortedListBasedLSMTFactory.INSTANCE.newSSTableReader(index, meta);
		treader.init();
		readerVerify(treader, conf, 0);
	}

	public static void readerVerify(ISSTableReader reader, Configuration conf, int level) throws IOException {
		int expect = (conf.getFlushLimit() + 1) * (1 << level);
		int size = 0;
		Iterator<WritableComparableKey> iter = reader.keySetIter();
		while (iter.hasNext()) {
			WritableComparableKey key = iter.next();
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
