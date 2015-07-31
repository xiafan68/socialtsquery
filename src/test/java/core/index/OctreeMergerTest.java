package core.index;

import java.io.IOException;

import org.junit.Test;

import Util.Configuration;
import core.index.MemTable.SSTableMeta;
import core.index.octree.OctreeMerger;
import core.index.octree.OctreeNode;

public class OctreeMergerTest {
	@Test
	public void merge2SStables() throws IOException {
		Configuration conf = new Configuration();
		conf.load("conf/index.conf");
		LSMOInvertedIndex index = new LSMOInvertedIndex(conf);
		DiskSSTableReader lhs = new DiskSSTableReader(index, new SSTableMeta(0,
				0));
		lhs.init();
		DiskSSTableReader rhs = new DiskSSTableReader(index, new SSTableMeta(1,
				0));
		rhs.init();

		OctreeMerger merge = new OctreeMerger(lhs.getPostingListScanner(0),
				rhs.getPostingListScanner(0));
		while (merge.hasNext()) {
			OctreeNode cur = merge.next();
			System.out.println(cur);
		}
	}

	@Test
	public void merge3SStables() throws IOException {
		Configuration conf = new Configuration();
		conf.load("conf/index.conf");
		LSMOInvertedIndex index = new LSMOInvertedIndex(conf);
		DiskSSTableReader lhs = new DiskSSTableReader(index, new SSTableMeta(
				32, 0));
		lhs.init();
		DiskSSTableReader rhs = new DiskSSTableReader(index, new SSTableMeta(
				33, 0));
		rhs.init();

		OctreeMerger merge = new OctreeMerger(lhs.getPostingListScanner(0),
				rhs.getPostingListScanner(0));
		DiskSSTableReader rrhs = new DiskSSTableReader(index, new SSTableMeta(
				34, 0));
		rrhs.init();
		OctreeMerger merge3 = new OctreeMerger(merge,
				rrhs.getPostingListScanner(0));
		while (merge3.hasNext()) {
			OctreeNode cur = merge3.next();
			System.out.println(cur);
		}
	}
}
