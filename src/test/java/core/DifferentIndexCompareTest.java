package core;

import java.io.IOException;
import java.util.Iterator;

import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

import core.commom.WritableComparable;
import core.lsmo.octree.IOctreeIterator;
import core.lsmo.octree.OctreeNode;
import core.lsmt.LSMTInvertedIndex;
import core.lsmt.postinglist.ISSTableReader;
import util.Configuration;

public class DifferentIndexCompareTest {
	private static final Logger logger = Logger.getLogger(DifferentIndexCompareTest.class);

	/**
	 * After loading the same dataset, compare whether the octreesof BDB and
	 * MarkFile are the same
	 * 
	 * @throws IOException
	 */
	@Test
	public void testBdbAndMarkFile() throws IOException {
		Configuration bdbConf = new Configuration();
		int index = 4;
		bdbConf.load("conf/weibo/lsmo_bdb_scale/index_lsmo_bdb_weibo_part20.conf");
		LSMTInvertedIndex bdbIndex = new LSMTInvertedIndex(bdbConf);
		bdbIndex.init();
		ISSTableReader bdbReader = bdbIndex.getSSTableReader(bdbIndex.getVersion(),
				bdbIndex.getVersion().diskTreeMetas.get(index));

		Configuration internConf = new Configuration();
		internConf.load("conf/weibo/lsmo_scale/index_lsmo_weibo_part20.conf");
		LSMTInvertedIndex internIndex = new LSMTInvertedIndex(internConf);
		internIndex.init();
		ISSTableReader internReader = internIndex.getSSTableReader(internIndex.getVersion(),
				internIndex.getVersion().diskTreeMetas.get(index));

		try {
			Iterator<WritableComparable> bdbIter = bdbReader.keySetIter();
			Iterator<WritableComparable> internIter = internReader.keySetIter();

			WritableComparable bdbKey = null;
			WritableComparable internKey = null;
			while (bdbIter.hasNext()) {
				bdbKey = bdbIter.next();
				internKey = internIter.next();
				logger.info(String.format("comparing key %s, %s", bdbKey, internKey));
				if (bdbKey.compareTo(internKey) != 0) {
					logger.info("key is not the same");
				} else {
					IOctreeIterator bdbOctreeIter = (IOctreeIterator) bdbReader.getPostingListScanner(bdbKey);
					IOctreeIterator internOctreeIter = (IOctreeIterator) internReader.getPostingListScanner(bdbKey);

					while (bdbOctreeIter.hasNext() && internOctreeIter.hasNext()) {
						OctreeNode bdbNode = bdbOctreeIter.nextNode();
						OctreeNode internNode = internOctreeIter.nextNode();

						if (bdbNode.getEncoding().compareTo(internNode.getEncoding()) != 0) {
							System.out.println();
							Assert.assertTrue(false);
						}
					}
					bdbOctreeIter.close();
					internOctreeIter.close();
				}
			}
		} finally {
			bdbIndex.close();
			internIndex.close();
		}
	}
}
