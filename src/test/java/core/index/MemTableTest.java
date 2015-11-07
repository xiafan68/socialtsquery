package core.index;

import java.io.IOException;

import org.junit.Test;

import Util.Configuration;
import common.MidSegment;
import core.lsmo.OctreeBasedLSMTFactory;
import core.lsmo.OctreeMemTable;
import core.lsmt.IMemTable.SSTableMeta;
import core.lsmt.LSMTInvertedIndex;
import core.lsmt.WritableComparableKey;
import fanxia.file.DirLineReader;

public class MemTableTest {
	@Test
	public void insertTest() throws IOException {
		Configuration conf = new Configuration();
		conf.load("conf/index.conf");
		LSMTInvertedIndex index = new LSMTInvertedIndex(conf);
		OctreeMemTable table = new OctreeMemTable(index, new SSTableMeta(0, 0));
		// "/Users/xiafan/Documents/dataset/expr/twitter/twitter_segs");
		DirLineReader reader = new DirLineReader("/home/xiafan/dataset/twitter/twitter_segs");
		String line = null;
		int i = 0;
		while (null != (line = reader.readLine())) {
			MidSegment seg = new MidSegment();
			seg.parse(line);
			// int keyCode = index.getKeywordCode(Long.toString(seg.getMid()));
			table.insert(new WritableComparableKey.StringKey(Long.toString(seg.getMid())), seg);
			if (i++ > conf.getFlushLimit())
				break;
		}
		DiskSSTableReaderTest.readerTest(table.getReader(), conf, 0);
	}
}
