package core.index;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.PropertyConfigurator;
import org.junit.Test;

import common.MidSegment;
import core.lsmo.OctreeBasedLSMTFactory;
import core.lsmo.OctreeMemTable;
import core.lsmo.octree.OctreeNode;
import core.lsmo.octree.OctreeNode.CompressedSerializer;
import core.lsmt.IMemTable;
import core.lsmt.IMemTable.SSTableMeta;
import core.lsmt.ISSTableWriter;
import core.lsmt.LSMTInvertedIndex;
import core.lsmt.WritableComparable;
import io.DirLineReader;
import util.Configuration;

public class SSTableWriterTest {
	@Test
	public void writeLevelOne() throws IOException {
		PropertyConfigurator.configure("conf/log4j-server.properties");
		Configuration conf = new Configuration();
		conf.load("conf/index.conf");

		LSMTInvertedIndex index = new LSMTInvertedIndex(conf);
		OctreeNode.HANDLER = CompressedSerializer.INSTANCE;
		DirLineReader reader = new DirLineReader("/home/xiafan/dataset/twitter/twitter_segs");
		String line = null;
		SSTableMeta meta = new SSTableMeta(0, 0);
		OctreeMemTable tree = new OctreeMemTable(index, meta);
		while (null != (line = reader.readLine())) {
			MidSegment seg = new MidSegment();
			seg.parse(line);
			tree.insert(new WritableComparable.StringKey(Long.toString(seg.getMid())), seg);
			if (tree.size() == conf.getFlushLimit()) {
				break;
			}
		}
		reader.close();

		List<IMemTable> tables = new ArrayList<IMemTable>();
		tables.add(tree);
		ISSTableWriter writer = OctreeBasedLSMTFactory.INSTANCE.newSSTableWriterForFlushing(tables, conf);
		writer.open(conf.getIndexDir());
		writer.write();
		writer.close();
	}
}
