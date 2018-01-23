package core.lsmo.internformat;

import java.io.IOException;

import org.apache.log4j.PropertyConfigurator;

import core.commom.WritableComparable;
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

	public InternOctreeSSTableReader(Configuration conf, SSTableMeta meta) {
		super(conf, meta);
	}

	@Override
	public IPostingListIterator getPostingListIter(WritableComparable key, int start, int end) throws IOException {
		return new InternPostingListIter(getDirEntry(key), this, start, end);
	}

	public static void main(String[] args) throws IOException {
		PropertyConfigurator.configure("conf/log4j-server.properties");
		Configuration conf = new Configuration();
		conf.load("conf/index_twitter_intern.conf");
		LSMTInvertedIndex index = new LSMTInvertedIndex(conf);
		InternOctreeSSTableReader reader = new InternOctreeSSTableReader(conf, new SSTableMeta(255, 7));
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
	public Pair<WritableComparable, BucketID> floorOffset(WritableComparable curKey, WritableComparable curCode)
			throws IOException {
		return null;
	}

	@Override
	public Pair<WritableComparable, BucketID> cellOffset(WritableComparable curKey,
			WritableComparable curCode) throws IOException {
		return null;
	}

	@Override
	public void closeIndex() throws IOException {
	}
}