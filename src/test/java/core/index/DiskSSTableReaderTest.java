package core.index;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.RegexFileFilter;
import org.junit.Assert;
import org.junit.Test;

import Util.Configuration;
import common.MidSegment;
import core.lsmo.DiskSSTableReader;
import core.lsmo.OctreeBasedLSMTFactory;
import core.lsmo.octree.IOctreeIterator;
import core.lsmo.octree.OctreeNode;
import core.lsmo.octree.OctreeNode.CompressedSerializer;
import core.lsmt.IMemTable.SSTableMeta;
import core.lsmt.ISSTableReader;
import core.lsmt.LSMTInvertedIndex;

public class DiskSSTableReaderTest {
	/**
	 * 遍历一个索引文件
	 * 
	 * @throws IOException
	 */
	@Test
	public void exampleSingleFile() throws IOException {
		OctreeNode.HANDLER = CompressedSerializer.INSTANCE;
		Configuration conf = new Configuration();
		conf.load("conf/index.conf");
		LSMTInvertedIndex index = new LSMTInvertedIndex(conf,
				OctreeBasedLSMTFactory.INSTANCE);
		File dataDir = conf.getIndexDir();
		List<File> files = new ArrayList<File>(FileUtils.listFiles(dataDir,
				new RegexFileFilter("[0-9]+_[0-9]+.data"), null));
		;
		Collections.sort(files, new Comparator<File>() {
			@Override
			public int compare(File o1, File o2) {
				int[] v1 = LSMTInvertedIndex.parseVersion(o1);
				int[] v2 = LSMTInvertedIndex.parseVersion(o2);
				int ret = Integer.compare(v1[1], v2[1]);
				if (ret == 0) {
					ret = Integer.compare(v1[0], v2[0]);
				}
				return ret;
			}
		});
		for (File dataFile : files) {
			System.out.println("examine data file of version:" + dataFile);
			int[] version = LSMTInvertedIndex.parseVersion(dataFile);
			DiskSSTableReader reader = new DiskSSTableReader(index,
					new SSTableMeta(version[0], version[1]));

			reader.init();
			readerTest(reader, conf, version[1]);
			reader.close();
		}
	}

	/**
	 * 遍历一个索引文件
	 * 
	 * @throws IOException
	 */
	@Test
	public void dumpDataFile() throws IOException {
		System.setOut(new PrintStream(new FileOutputStream("/tmp/7_0.txt")));
		Configuration conf = new Configuration();
		conf.load("conf/index.conf");
		LSMTInvertedIndex index = new LSMTInvertedIndex(conf,
				OctreeBasedLSMTFactory.INSTANCE);
		DiskSSTableReader reader = new DiskSSTableReader(index,
				new SSTableMeta(131, 1));
		reader.init();
		readerTest(reader, conf, 1);

		Iterator<Integer> iter = reader.keySetIter();
		OctreeNode pre = null;
		while (iter.hasNext()) {
			int key = iter.next();
			if (key != 5)
				continue;
			System.out.println("scanning postinglist of " + key);
			pre = null;
			IOctreeIterator scanner = reader.getPostingListScanner(key);
			OctreeNode cur = null;
			while (scanner.hasNext()) {
				cur = scanner.nextNode();
				System.out.println(cur);
				if (pre != null) {
					if (pre.getEncoding().compareTo(cur.getEncoding()) >= 0)
						System.out.println(key + "\n" + pre + "\n" + cur);
					Assert.assertTrue(pre.getEncoding().compareTo(
							cur.getEncoding()) < 0);
				}
				pre = cur;
			}
		}
		reader.close();

	}

	/**
	 * 遍历一个索引文件
	 * 
	 * @throws IOException
	 */
	@Test
	public void detectMissingSegs() throws IOException {
		Configuration conf = new Configuration();
		conf.load("conf/index.conf");
		LSMTInvertedIndex index = new LSMTInvertedIndex(conf,
				OctreeBasedLSMTFactory.INSTANCE);
		int level = 7;
		int expect = (conf.getFlushLimit() + 1) * (1 << level);
		DiskSSTableReader reader = null;
		HashSet<MidSegment> segs = new HashSet<MidSegment>();

		/*
		 * reader = new DiskSSTableReader(index, new SSTableMeta(127, level));
		 * reader.init(); readerTest(reader, segs); reader = new
		 * DiskSSTableReader(index, new SSTableMeta(255, level)); reader.init();
		 * readerTest(reader, segs); System.out.println(segs.size());
		 */

		level = 8;
		reader = new DiskSSTableReader(index, new SSTableMeta(255, level));
		reader.init();
		HashSet<MidSegment> mergeSegs = new HashSet<MidSegment>();
		// readerTest(reader, mergeSegs);
		segs.removeAll(mergeSegs);
		System.out.println("not find " + segs);
		readerTest(reader, conf, level);
	}

	public static void readerTest(ISSTableReader reader,
			HashSet<MidSegment> segs) throws IOException {
		Iterator<Integer> iter = reader.keySetIter();
		while (iter.hasNext()) {
			int key = iter.next();
			IOctreeIterator scanner = (IOctreeIterator) reader
					.getPostingListScanner(key);
			OctreeNode cur = null;
			while (scanner.hasNext()) {
				cur = scanner.nextNode();

				for (MidSegment seg : cur.getSegs()) {
					if (segs.contains(seg))
						System.err.println("existing " + seg);
					segs.add(seg);
				}
			}
		}
	}

	public static void readerTest(ISSTableReader reader, Configuration conf,
			int level) throws IOException {
		int expect = (conf.getFlushLimit() + 1) * (1 << level);
		int size = 0;
		Iterator<Integer> iter = reader.keySetIter();
		OctreeNode pre = null;
		// HashSet<MidSegment> segs = new HashSet<MidSegment>();
		while (iter.hasNext()) {
			int key = iter.next();
			System.out.println("scanning postinglist of " + key);
			pre = null;
			IOctreeIterator scanner = (IOctreeIterator) reader
					.getPostingListScanner(key);
			OctreeNode cur = null;
			int count = 0;
			while (scanner.hasNext()) {
				cur = scanner.nextNode();
				count++;
				if (pre != null) {
					if (pre.getEncoding().compareTo(cur.getEncoding()) >= 0)
						System.out.println("count " + count + " " + key + "\n"
								+ pre + "\n" + cur);
					Assert.assertTrue(pre.getEncoding().compareTo(
							cur.getEncoding()) < 0);
				}
				/*
				 * for (MidSegment seg : cur.getSegs()) { if
				 * (segs.contains(seg)) { System.err.println(seg); }
				 * segs.add(seg); }
				 */
				pre = cur;
				size += cur.size();
				// System.out.println(key + " " + cur);
			}
			System.out.println("expect size:" + expect + " cursize size:"
					+ size);

		}
		if (expect != size)
			System.err.println("expect size:" + expect + " total size:" + size);
		Assert.assertEquals(expect, size);
	}
}
