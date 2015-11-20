package core.lsmo;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.log4j.PropertyConfigurator;
import org.junit.Assert;
import org.junit.Test;

import core.commom.BDBBtree.BDBKeyIterator;
import core.lsmo.internformat.BlockBasedSSTableReader;
import core.lsmo.octree.IOctreeIterator;
import core.lsmt.IMemTable.SSTableMeta;
import core.lsmt.LSMTInvertedIndex;
import core.lsmt.WritableComparableKey;
import segmentation.Interval;
import util.Configuration;

public class IndexValidation {

	public static LSMTInvertedIndex openIndex(String logPath, String confPath) throws IOException {
		PropertyConfigurator.configure(logPath);
		Configuration conf = new Configuration();
		conf.load(confPath);
		LSMTInvertedIndex index = new LSMTInvertedIndex(conf);
		try {
			index.init();
		} catch (IOException e) {
		}
		return index;
	}

	/**
	 * 验证iter可以扫描到所有的数据
	 * 
	 * @throws IOException
	 */
	@Test
	public void validateScanner() throws IOException {
		LSMTInvertedIndex index = openIndex("conf/log4j-server.properties", "conf/index_twitter_intern.conf");
		// FileOutputStream fos = new FileOutputStream("../../error.txt");
		// System.setOut(new PrintStream(fos));
		for (SSTableMeta meta : index.getVersion().diskTreeMetas) {
			BlockBasedSSTableReader reader = (BlockBasedSSTableReader) index.getSSTableReader(index.getVersion(), meta);
			Iterator<WritableComparableKey> iter = reader.keySetIter();
			int start = 696000;
			int end = 699100;
			int k = 10;
			WritableComparableKey key;
			while (iter.hasNext()) {
				key = iter.next();
				try {
					IOctreeIterator octIter = (IOctreeIterator) reader.getPostingListScanner(key);
					int expectedCount = 0;
					while (octIter.hasNext()) {
						expectedCount += octIter.next().getValue().size();
					}
					octIter = (IOctreeIterator) reader.getPostingListIter(key, 0, Integer.MAX_VALUE / 2);
					int iterCount = 0;
					while (octIter.hasNext()) {
						iterCount += octIter.next().getValue().size();
					}
					System.out.println("verify key:" + key + " expected:" + expectedCount + " actural:" + iterCount);
					Assert.assertEquals(expectedCount, iterCount);
				} catch (Exception ex) {
					System.out.println(meta + "\t" + key);
				}
			}
			((BDBKeyIterator) iter).close();
		}
		index.close();
		// fos.close();
	}

	@Test
	public void validate() throws IOException {
		LSMTInvertedIndex index = openIndex("conf/log4j-server.properties", "conf/index_twitter_intern.conf");
		FileOutputStream fos = new FileOutputStream("../../error.txt");
		System.setOut(new PrintStream(fos));
		for (SSTableMeta meta : index.getVersion().diskTreeMetas) {
			BlockBasedSSTableReader reader = (BlockBasedSSTableReader) index.getSSTableReader(index.getVersion(), meta);
			Iterator<WritableComparableKey> iter = reader.keySetIter();
			int start = 696000;
			int end = 699100;
			int k = 10;
			WritableComparableKey key;
			while (iter.hasNext()) {
				key = iter.next();
				try {
					Iterator<Interval> invs = index.query(Arrays.asList(key.toString()), start, end, k, "WEIGHTED");
					if (invs.hasNext()) {
						Interval inv = invs.next();
						// System.out.println("has " + inv);
						index.query(Arrays.asList(key.toString()), inv.getStart(), inv.getEnd(), k, "WEIGHTED");
					}
				} catch (Exception ex) {
					System.out.println(meta + "\t" + key);
				}
			}
			((BDBKeyIterator) iter).close();
		}
		index.close();
		fos.close();
	}

	private static void validate(LSMTInvertedIndex indexA, LSMTInvertedIndex indexB) throws IOException {
		int ts = 0;
		int te = 700000;
		int topk = 300;
		HashMap<Long, Interval> bdb = new HashMap<Long, Interval>();
		Iterator<Interval> iter = indexA.query(Arrays.asList("time"), ts, te, topk, "weighted".toUpperCase());
		while (iter.hasNext()) {
			Interval inv = iter.next();
			bdb.put(inv.getMid(), inv);
		}
		System.out.println("size of bdb:" + bdb.size());

		HashMap<Long, Interval> intern = new HashMap<Long, Interval>();
		iter = indexB.query(Arrays.asList("time"), ts, te, topk, "weighted".toUpperCase());
		while (iter.hasNext()) {
			Interval inv = iter.next();
			if (bdb.containsKey(inv.getMid())) {
				bdb.remove(inv.getMid());
			} else {
				intern.put(inv.getMid(), inv);
			}
		}

		for (Interval inv : bdb.values()) {
			System.out.println(inv);
		}
		System.out.println("_______________________________");
		// System.out.println(intern);
		for (Interval inv : intern.values()) {
			System.out.println(inv);
		}

		System.out.println(bdb.size() + "," + intern.size());
	}

	@Test
	public void crossValidateIntern() throws IOException {
		LSMTInvertedIndex bdbIndex = openIndex("conf/log4j-server2.properties", "conf/index_twitter.conf");
		LSMTInvertedIndex internIndex = openIndex("conf/log4j-server2.properties", "conf/index_twitter_intern.conf");
		validate(bdbIndex, internIndex);
		bdbIndex.close();
		internIndex.close();
	}

	@Test
	public void crossValidateLSMI() throws IOException {
		LSMTInvertedIndex bdbIndex = openIndex("conf/log4j-server2.properties", "conf/index_twitter.conf");
		LSMTInvertedIndex internIndex = openIndex("conf/log4j-server2.properties", "conf/index_twitter_lsmi.conf");
		validate(bdbIndex, internIndex);
		bdbIndex.close();
		internIndex.close();
	}

}
