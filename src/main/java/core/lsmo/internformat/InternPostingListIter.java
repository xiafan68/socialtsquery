package core.lsmo.internformat;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.log4j.Logger;

import common.MidSegment;
import core.commom.Encoding;
import core.commom.Point;
import core.io.Block;
import core.io.Block.BLOCKTYPE;
import core.io.Bucket;
import core.io.Bucket.BucketID;
import core.lsmo.octree.IOctreeIterator;
import core.lsmo.octree.OctreeNode;
import core.lsmt.ISSTableWriter.DirEntry;
import core.lsmt.PostingListMeta;
import core.lsmt.WritableComparableKey;
import io.ByteUtil;
import util.Pair;

public class InternPostingListIter implements IOctreeIterator {
	private static final Logger logger = Logger.getLogger(InternPostingListIter.class);
	private DirEntry entry;
	private BlockBasedSSTableReader reader;
	private int ts;
	private int te;

	private Encoding curMin;
	private Encoding curMax;
	private Encoding max;

	BucketID nextID = new BucketID(0, (short) 0); // 下一个需要读取的octant
	private Bucket curBuck = new Bucket(Integer.MIN_VALUE); // 当前这个bucket
	private int nextBlockID = 0; // 下一个bucket的blockid

	private OctreeNode curNode = null;
	DataInputStream input = null;

	TreeMap<Integer, SkipCell> skipMeta = new TreeMap<Integer, SkipCell>();
	int curSkipBlockIdx;

	/*
	 * PriorityQueue<OctreeNode> traverseQueue = new
	 * PriorityQueue<OctreeNode>(256, new Comparator<OctreeNode>() {
	 * 
	 * @Override public int compare(OctreeNode o1, OctreeNode o2) { return
	 * o1.getEncoding().compareTo(o2.getEncoding()); } });
	 */

	/**
	 * 
	 * @param reader
	 * @param ts
	 * @param te
	 */
	public InternPostingListIter(DirEntry entry, BlockBasedSSTableReader reader, int ts, int te) {
		if (entry != null && entry.curKey != null && entry.minTime <= te && entry.maxTime >= ts) {
			this.entry = entry;
			this.reader = reader;
			this.ts = ts;
			this.te = te;
			curMin = new Encoding(new Point(0, ts, Integer.MAX_VALUE), 0);
			curMax = new Encoding(new Point(te, Integer.MAX_VALUE, Integer.MAX_VALUE), 0);
			max = new Encoding(new Point(te, Integer.MAX_VALUE, 0), 0);
			nextID.copy(entry.startBucketID);
			// nextBlockID = entry.startBucketID.blockID;
			curSkipBlockIdx = DirEntry.indexBlockIdx(entry.indexStartOffset);
		}
	}

	@Override
	public PostingListMeta getMeta() {
		return entry;
	}

	@Override
	public void open() throws IOException {

	}

	@Override
	public boolean hasNext() throws IOException {
		if (entry != null && entry.curKey != null && curNode == null && diskHasMore()) {
			advance();
		}
		return curNode != null;
	}

	private void gotoNewLayer(int layer) {
		curMin.setPaddingBitNum(0);
		curMin.setX(0);
		curMin.setY(ts);
		curMin.setTop(layer);

		curMax.setPaddingBitNum(0);
		curMax.setX(te);
		curMax.setY(Integer.MAX_VALUE);
		curMax.setTop(layer);
	}

	private boolean isCorrectCode(Encoding newCode) {
		boolean ret = false;
		if (newCode.getX() <= te && newCode.getY() + newCode.getEdgeLen() > ts) {
			ret = true;
			nextID.offset++;
		} else if (newCode.getTopZ() != curMin.getTopZ() || newCode.compareTo(curMax) > 0) {
			// newCode.getTopZ() != curMin.getTopZ()
			// 应该也一定要能够推出newCode.compareTo(curMax) > 0
			if (newCode.getTopZ() != curMin.getTopZ()) {
				gotoNewLayer(newCode.getTopZ());
			} else {
				nextID.offset++;
				gotoNewLayer(newCode.getTopZ() - 1);
			}
		} else if (newCode.getX() > te) {
			// 这个走向下一层的标准不对，应该计算当前的面的最大值，只有大于这个最大值时才走向下一个面
			// 我们需要往回，往上跳,只有公共祖先上侧的点才有可能被访问
			// 先找到公共祖先
			int commBit = ByteUtil.commonBitNum(newCode.getX(), te);
			int newX = ByteUtil.fetchHeadBits(newCode.getX(), commBit);
			int newY = ByteUtil.fetchHeadBits(newCode.getY(), commBit);
			newY |= 1 << (31 - commBit + 1); // 这里应该是找公共祖先的上面的那个节点
			curMin.setPaddingBitNum(0);
			curMin.setX(newX);
			curMin.setY(newY);
			curMin.setTop(newCode.getTopZ());
			nextID.offset++;
		} else if (newCode.getY() + newCode.getEdgeLen() <= ts) {
			int commBit = ByteUtil.commonBitNum(newCode.getY(), ts);
			int newX = ByteUtil.fetchHeadBits(newCode.getX(), commBit);
			int newY = ByteUtil.fetchHeadBits(newCode.getY(), commBit);
			// 选择上子节点
			newY |= 1 << (31 - commBit);
			// 选择和当前node在左右方向上统一侧的节点
			newX |= (1 << (31 - commBit)) & newCode.getX();
			curMin.setPaddingBitNum(0);
			curMin.setY(newY);
			curMin.setX(newX);
			curMin.setTop(newCode.getTopZ());
			nextID.offset++;
		}

		return ret;
	}

	private void loadMetaBlock(Block block) throws IOException {
		SkipCell cell = new SkipCell(block.getBlockIdx(), reader.getFactory());
		cell.read(block);
		skipMeta.put(block.getBlockIdx(), cell);
	}

	private void loadMetaBlock(int blockIdx) throws IOException {
		Block block = new Block(BLOCKTYPE.META_BLOCK, blockIdx);
		reader.getBlockFromDataFile(block);
		SkipCell cell = new SkipCell(block.getBlockIdx(), reader.getFactory());
		cell.read(block);
		skipMeta.put(block.getBlockIdx(), cell);
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

	private Pair<WritableComparableKey, BucketID> cellOffset() throws IOException {
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
	public void readNextBucket() throws IOException {
		boolean lastBlock = false;
		List<Block> blocks = new ArrayList<Block>();
		curBuck.setBlockIdx(nextID.blockID);
		do {
			Block block = new Block(Block.BLOCKTYPE.DATA_BLOCK, 0);
			block.setBlockIdx(nextBlockID);
			nextBlockID = reader.getBlockFromDataFile(block);
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
	/**
	 * 开始的时候，用curMin去找到第一个bucket
	 * 
	 * 一直往前跑， 如果当前跑到查询区间下面去了，生成新的code，跳转 如果当前跑到查询区间的外面去了，生成新的code，跳转
	 * 
	 */

	/**
	 * 在index上找到第一个大于等于参数的octant
	 * 
	 * @throws IOException
	 */
	private boolean skipToOctant() throws IOException {
		// searching in current meta blocks

		Pair<WritableComparableKey, BucketID> pair = cellOffset();
		if (pair.getKey() != null) {
			// 利用pair跳转，nextID
			if (curBuck.octNum() == 0 || nextID.compareTo(pair.getValue()) < 0) {
				curBuck.reset();
				nextID.copy(pair.getValue());
			}

			while (diskHasMore()) {
				Encoding code = readNextOctantCode();
				if (code.contains(curMin) || code.compareTo(curMin) >= 0) {
					break;
				} else {
					nextID.offset++;
				}
			}
			return true;
		} else {
			return false;
		}
	}

	/**
	 * 只负责顺序的读取下一个octant
	 * 
	 * @throws IOException
	 */
	private Encoding readNextOctantCode() throws IOException {
		if (curBuck.blockIdx().blockID < 0 || nextID.blockID != curBuck.blockIdx().blockID) {
			curBuck.reset();
			nextBlockID = nextID.blockID;
			readNextBucket();
		} else if (nextID.offset >= curBuck.octNum()) {
			curBuck.reset();
			nextID.blockID = nextBlockID;
			nextID.offset = 0;
			readNextBucket();
		}

		Encoding curCode = new Encoding();
		byte[] data = curBuck.getOctree(nextID.offset);
		input = new DataInputStream(new ByteArrayInputStream(data));
		curCode.read(input);
		return curCode;
	}

	private void readNextOctant(Encoding curCode) throws IOException {
		curNode = new OctreeNode(curCode, curCode.getEdgeLen());
		curNode.read(input);

		/*
		 * if (!traverseQueue.isEmpty() &&
		 * curNode.getEncoding().compareTo(traverseQueue.peek().getEncoding()) >
		 * 0) { traverseQueue.offer(curNode); curNode = traverseQueue.poll(); }
		 */

		input.close();
		input = null;
	}

	/**
	 * 判断是否还有必要读取
	 * 
	 * @return
	 */
	private boolean diskHasMore() {
		return curMin.compareTo(max) <= 0 && nextID.compareTo(entry.endBucketID) <= 0;
	}

	private void advance() throws IOException {
		while (curNode == null && diskHasMore()) {
			Encoding curCode = readNextOctantCode();
			// 开始判断code是否在查询区间内
			if (isCorrectCode(curCode)) {
				readNextOctant(curCode);
			} else {
				skipToOctant();
			}
		}
	}

	public OctreeNode nextNode() throws IOException {
		advance();
		OctreeNode ret = curNode;
		curNode = null;
		return ret;
	}

	@Override
	public void close() throws IOException {
	}

	@Override
	public Pair<Integer, List<MidSegment>> next() throws IOException {
		OctreeNode node = nextNode();
		return new Pair<Integer, List<MidSegment>>(node.getEncoding().getTopZ(),
				new ArrayList<MidSegment>(node.getSegs()));
	}

	@Override
	public void skipTo(WritableComparableKey key) throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void addNode(OctreeNode node) {
		throw new RuntimeException("unimplemented addNode");
		// traverseQueue.add(node);
	}

}
