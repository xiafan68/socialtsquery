package core.index;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.PropertyConfigurator;
import org.junit.Test;

import Util.Configuration;

import common.MidSegment;

import core.lsmo.OctreeBasedLSMTFactory;
import core.lsmo.OctreeMemTable;
import core.lsmo.octree.OctreeNode;
import core.lsmo.octree.OctreeNode.CompressedSerializer;
import core.lsmt.IMemTable;
import core.lsmt.IMemTable.SSTableMeta;
import core.lsmt.ISSTableWriter;
import core.lsmt.LSMTInvertedIndex;
import fanxia.file.DirLineReader;

public class SSTableWriterTest {
	@Test
	public void writeLevelOne() throws IOException {
		PropertyConfigurator.configure("conf/log4j-server.properties");
		Configuration conf = new Configuration();
		conf.load("conf/index.conf");

		LSMTInvertedIndex index = new LSMTInvertedIndex(conf,
				OctreeBasedLSMTFactory.INSTANCE);
		OctreeNode.HANDLER = CompressedSerializer.INSTANCE;
		DirLineReader reader = new DirLineReader(
				"/home/xiafan/dataset/twitter/twitter_segs");
		String line = null;
		SSTableMeta meta = new SSTableMeta(0, 0);
		OctreeMemTable tree = new OctreeMemTable(index, meta);
		while (null != (line = reader.readLine())) {
			MidSegment seg = new MidSegment();
			seg.parse(line);
			tree.insert(Math.abs(Long.toString(seg.getMid()).hashCode()) % 10,
					seg);
			if (tree.size() == conf.getFlushLimit()) {
				break;
			}
		}
		reader.close();

		List<IMemTable> tables = new ArrayList<IMemTable>();
		tables.add(tree);
		ISSTableWriter writer = OctreeBasedLSMTFactory.INSTANCE
				.newSSTableWriterForFlushing(tables, 128);
		writer.write(conf.getIndexDir());
		writer.close();
	}
}
