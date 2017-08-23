package core.lsmo.internformat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.log4j.Logger;

import core.io.Block;
import core.io.Block.BLOCKTYPE;
import core.io.Bucket.BucketID;
import core.lsmo.octree.OctreePostingListIter;
import core.lsmt.WritableComparableKey;
import core.lsmt.postinglist.ISSTableWriter.DirEntry;
import util.Pair;

public class InternPostingListIter extends OctreePostingListIter {
	private static final Logger logger = Logger.getLogger(InternPostingListIter.class);

	TreeMap<Integer, SkipCell> skipMeta = new TreeMap<Integer, SkipCell>();
	int curSkipBlockIdx;

	/**
	 * 
	 * @param reader
	 * @param ts
	 * @param te
	 */
	public InternPostingListIter(DirEntry entry, BlockBasedSSTableReader reader, int ts, int te) {
		super(entry, reader, ts, te);
		if (entry != null && entry.curKey != null && entry.minTime <= te && entry.maxTime >= ts) {
			curSkipBlockIdx = DirEntry.indexBlockIdx(entry.indexStartOffset);
		}
	}

	/**
	 * deserialzie skiplist from index block
	 * 
	 * @param block
	 * @throws IOException
	 */
	private void loadMetaBlock(Block block) throws IOException {
		SkipCell cell = new SkipCell(block.getBlockIdx(), ((BlockBasedSSTableReader) reader).getFactory());
		cell.read(block);
		skipMeta.put(block.getBlockIdx(), cell);
	}

	private void loadMetaBlock(int blockIdx) throws IOException {
		Block block = new Block(BLOCKTYPE.META_BLOCK, blockIdx);
		((BlockBasedSSTableReader) reader).getBlockFromDataFile(block);
		loadMetaBlock(block);
	}

	private void readMetaBlockAndClearCache(int nextIdx) throws IOException {
		if (!skipMeta.containsKey(nextIdx)) {
			loadMetaBlock(nextIdx);
		}

		Integer preKey = skipMeta.lowerKey(nextIdx - 1);
		if (preKey != null) {
			// clear cache
			Iterator<Entry<Integer, SkipCell>> iter = skipMeta.entrySet().iterator();
			while (iter.hasNext()) {
				Entry<Integer, SkipCell> entry = iter.next();
				if (entry.getKey() < preKey.intValue()) {
					iter.remove();
				} else {
					break;
				}
			}
		}
	}

	@Override
	protected Pair<WritableComparableKey, BucketID> cellOffset() throws IOException {
		Pair<WritableComparableKey, BucketID> ret = new Pair<WritableComparableKey, BucketID>(null, null);
		SkipCell curCell = null;

		boolean hitFirst = false;
		int preSkipBlockIdx = -1;
		do {
			readMetaBlockAndClearCache(curSkipBlockIdx);
			curCell = skipMeta.get(curSkipBlockIdx);

			int skipIdx = 0;
			int start = curSkipBlockIdx == DirEntry.indexBlockIdx(entry.indexStartOffset)
					? DirEntry.indexOffsetInBlock(entry.indexStartOffset) : 0;
			int end = curSkipBlockIdx == DirEntry.indexBlockIdx(entry.sampleNum)
					? DirEntry.indexOffsetInBlock(entry.sampleNum) + 1 : curCell.size();
			skipIdx = curCell.cellOffset(curMin, start, end);

			if (skipIdx == curCell.size() - 1) {
				Pair<WritableComparableKey, BucketID> temp = curCell.getIndexEntry(skipIdx);
				ret.setKey(temp.getKey());
				ret.setValue(new BucketID(curCell.getBlockIdx() + temp.getValue().blockID + 1, temp.getValue().offset));
				if (hitFirst) {
					break;
				}
				if (curCell.nextMetaBlockIdx != -1) {
					preSkipBlockIdx = curSkipBlockIdx;
					curSkipBlockIdx = curCell.nextMetaBlockIdx;
				} else {
					break;
				}
			} else if (skipIdx != 0) {
				Pair<WritableComparableKey, BucketID> tmp = curCell.getIndexEntry(skipIdx);
				assert tmp.getKey().compareTo(curMin) <= 0;
				ret.setKey(tmp.getKey());
				ret.setValue(new BucketID(curCell.getBlockIdx() + tmp.getValue().blockID + 1, tmp.getValue().offset));
				break;
			} else {
				Pair<WritableComparableKey, BucketID> tmp = curCell.getIndexEntry(skipIdx);
				if (tmp.getKey().compareTo(curMin) <= 0) {
					ret.setKey(tmp.getKey());
					ret.setValue(
							new BucketID(curCell.getBlockIdx() + tmp.getValue().blockID + 1, tmp.getValue().offset));
					break;
				} else {
					// assert ret.getKey() != null;
					hitFirst = true;
					if (preSkipBlockIdx >= 0) {
						// 对于这种情况下，一定是在前一个block中已经找到了
						assert ret.getKey() != null;
						curSkipBlockIdx = preSkipBlockIdx;
						break;
					} else {

						if (skipMeta.lowerKey(curSkipBlockIdx - 1) != null)
							curSkipBlockIdx = skipMeta.lowerKey(curSkipBlockIdx - 1);
						else {
							// 这种情况也需要返回，说明最小的都时间点都比查询的curMin大
							break;
						}
					}
				}
			}
		} while (true);
		return ret;
	}

	/**
	 * 读取下一个bucket
	 * 
	 * @throws IOException
	 */
	@Override
	public void readNextBucket() throws IOException {
		boolean lastBlock = false;
		List<Block> blocks = new ArrayList<Block>();
		curBuck.setBlockIdx(nextID.blockID);
		do {
			Block block = new Block(Block.BLOCKTYPE.DATA_BLOCK, 0);
			block.setBlockIdx(nextBlockID);
			nextBlockID = ((BlockBasedSSTableReader) reader).getBlockFromDataFile(block);
			if (block.isDataBlock()) {
				blocks.add(block);
			} else {
				loadMetaBlock(block);
				if (blocks.isEmpty()) {// 必须在这里把nextID和curBuck的值设置正确，否这在判断是否遍历到posting
										// list结尾时会出错
					curBuck.setBlockIdx(nextBlockID);
					nextID.blockID = nextBlockID;
				}
				continue;
			}
			lastBlock = block.getData()[0] == 1;
		} while (!lastBlock);
		curBuck.read(blocks);
	}
}
