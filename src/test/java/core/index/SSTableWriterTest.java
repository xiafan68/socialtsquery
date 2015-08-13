package core.index;

import java.io.IOException;
import java.util.Arrays;

import org.apache.log4j.PropertyConfigurator;
import org.junit.Test;

import Util.Configuration;

import common.MidSegment;
import core.lsmo.MemTable;
import core.lsmo.SSTableWriter;
import core.lsmo.MemTable.SSTableMeta;
import core.lsmt.LSMOInvertedIndex;
import fanxia.file.DirLineReader;

public class SSTableWriterTest {
	@Test
	public void writeLevelOne() throws IOException {
		PropertyConfigurator.configure("conf/log4j-server.properties");
		Configuration conf = new Configuration();
		conf.load("conf/index.conf");

		LSMOInvertedIndex index = new LSMOInvertedIndex(conf);
		DirLineReader reader = new DirLineReader(
				"/Users/xiafan/Documents/dataset/expr/twitter/twitter_segs");
		String line = null;
		SSTableMeta meta = new SSTableMeta(0, 0);
		MemTable tree = new MemTable(index, meta);
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

		SSTableWriter writer = new SSTableWriter(Arrays.asList(tree), 128);
		writer.write(conf.getIndexDir());
		writer.close();
	}
}
