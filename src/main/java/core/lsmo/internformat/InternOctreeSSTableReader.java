package core.lsmo.internformat;

import java.io.IOException;

import org.apache.log4j.PropertyConfigurator;

import core.commom.WritableComparableKey;
import core.io.Block;
import core.io.Block.BLOCKTYPE;
import core.io.Bucket.BucketID;
import core.lsmt.IBucketBasedSSTableReader;
import core.lsmt.IMemTable.SSTableMeta;
import core.lsmt.LSMTInvertedIndex;
import core.lsmt.postinglist.IPostingListIterator;
import util.Configuration;
import util.Pair;

public class InternOctreeSSTableReader extends IBucketBasedSSTableReader {

	public InternOctreeSSTableReader(LSMTInvertedIndex index, SSTableMeta meta) {
		super(index, meta);
	}

	@Override
	protected void finalize() throws Throwable {
		close();
	};

	@Override
	public IPostingListIterator getPostingListIter(WritableComparableKey key, int start, int end) throws IOException {
		return new InternPostingListIter(getDirEntry(key), this, start, end);
	}

	public static void main(String[] args) throws IOException {
		PropertyConfigurator.configure("conf/log4j-server.properties");
		Configuration conf = new Configuration();
		conf.load("conf/index_twitter_intern.conf");
		LSMTInvertedIndex index = new LSMTInvertedIndex(conf);
		InternOctreeSSTableReader reader = new InternOctreeSSTableReader(index, new SSTableMeta(255, 7));
		reader.init();
		Block block = new Block(BLOCKTYPE.DATA_BLOCK, 0);
		for (int i = 516000; i < 527300; i++) {
			block.setBlockIdx(i);
			reader.getBlockFromDataFile(block);
			// System.out.print(i + "");
			if (block.isDataBlock()) {
				// System.out.println(" is data");
			} else {
				System.out.println(i + " is meta");
			}
		}
		reader.close();

	}

	@Override
	public void initIndex() throws IOException {
	}

	@Override
	public Pair<WritableComparableKey, BucketID> floorOffset(WritableComparableKey curKey, WritableComparableKey curCode)
			throws IOException {
		return null;
	}

	@Override
	public Pair<WritableComparableKey, BucketID> cellOffset(WritableComparableKey curKey,
			WritableComparableKey curCode) throws IOException {
		return null;
	}

	@Override
	public void closeIndex() throws IOException {
	}
}