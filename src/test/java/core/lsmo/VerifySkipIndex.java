package core.lsmo;

import java.io.IOException;
import java.util.Iterator;

import org.junit.Test;

import core.commom.BDBBtree;
import core.io.Block;
import core.io.Block.BLOCKTYPE;
import core.lsmo.internformat.BlockBasedSSTableReader;
import core.lsmo.internformat.SkipCell;
import core.lsmt.IMemTable.SSTableMeta;
import core.lsmt.ISSTableWriter.DirEntry;
import core.lsmt.LSMTInvertedIndex;
import core.lsmt.WritableComparableKey;
import segmentation.Interval;

public class VerifySkipIndex {
	@Test
	public void crossValidateIntern() throws IOException {
		LSMTInvertedIndex internIndex = IndexValidation.openIndex("conf/log4j-server2.properties",
				"conf/index_twitter_intern.conf");
		int ts = 676602;
		int te = 696622;
		Interval window = new Interval(0, ts, te, 0);
		try {
			for (SSTableMeta meta : internIndex.getVersion().diskTreeMetas) {
				BlockBasedSSTableReader reader = (BlockBasedSSTableReader) internIndex
						.getSSTableReader(internIndex.getVersion(), meta);
				Iterator<WritableComparableKey> iter = reader.keySetIter();
				while (iter.hasNext()) {
					WritableComparableKey key = iter.next();
					DirEntry entry = reader.getDirEntry(key);


				}
				((BDBBtree.BDBKeyIterator) iter).close();
			}
		} finally {
			//index.close();
		}
		internIndex.close();
	}
}
