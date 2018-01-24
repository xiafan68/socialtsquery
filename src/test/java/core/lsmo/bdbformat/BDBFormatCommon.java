package core.lsmo.bdbformat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.commons.collections.map.DefaultedMap;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.PropertyConfigurator;
import org.junit.After;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import common.MidSegment;
import common.TestDataGenerator;
import core.commom.WritableComparable;
import core.lsmo.OctreeMemTable;
import core.lsmo.octree.OctreeNode;
import core.lsmo.octree.OctreeNode.CompressedSerializer;
import core.lsmt.IMemTable;
import core.lsmt.IMemTable.SSTableMeta;
import core.lsmt.LSMTInvertedIndex;
import core.lsmt.postinglist.ISSTableWriter;
import util.Configuration;
import util.Pair;

public class BDBFormatCommon {
	protected static final Logger logger = LoggerFactory.getLogger(BDBFormatReaderWriterTest.class);
	protected LSMTInvertedIndex index;
	protected Configuration conf;

	@Before
	public void setupIndex() throws IOException {
		PropertyConfigurator.configure("conf/log4j-server.properties");
		conf = new Configuration();
		conf.load("conf/index_lsmo_bdb.conf");
		FileUtils.deleteDirectory(conf.getIndexDir());
		conf.getIndexDir().mkdirs();
		conf.getTmpDir().mkdirs();
		index = new LSMTInvertedIndex(conf);
		OctreeNode.HANDLER = CompressedSerializer.INSTANCE;
	}

	@After
	public void cleanUp() throws IOException {
		FileUtils.deleteDirectory(conf.getIndexDir());
	}

	protected void genDiskSSTable(SSTableMeta meta, TestDataGenerator gen, DefaultedMap termCounts, int sizeLimit)
			throws IOException {
		OctreeMemTable tree = new OctreeMemTable(index, meta);
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
			if (dataNum > sizeLimit)
				break;
		}

		@SuppressWarnings("rawtypes")
		List<IMemTable> tables = new ArrayList<IMemTable>();
		tables.add(tree);
		ISSTableWriter writer = OctreeBDBFormatLSMTFactory.INSTANCE.newSSTableWriterForFlushing(tables, conf);
		writer.open(conf.getIndexDir());
		writer.write();
		writer.close();
	}

}
