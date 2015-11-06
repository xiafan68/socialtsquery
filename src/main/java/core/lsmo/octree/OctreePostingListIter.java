package core.lsmo.octree;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import Util.Pair;
import common.MidSegment;
import core.commom.Encoding;
import core.commom.Point;
import core.io.Bucket;
import core.io.Bucket.BucketID;
import core.lsmt.IBucketBasedSSTableReader;
import core.lsmt.ISSTableWriter.DirEntry;
import core.lsmt.PostingListMeta;
import core.lsmt.WritableComparableKey;
import fanxia.file.ByteUtil;

/**
 * 用于访问disk上的octree
 * 
 * @author xiafan
 *
 */
public class OctreePostingListIter implements IOctreeIterator {
	private static final Logger logger = Logger.getLogger(OctreePostingListIter.class);
	private DirEntry entry;
	private IBucketBasedSSTableReader reader;
	private int ts;
	private int te;

	private Encoding curMin = null;
	private Encoding curMax = null;
	private Encoding max = null;

	BucketID nextID = new BucketID(0, (short) 0);
	private Bucket curBuck = new Bucket(0);
	private int nextBlockID = 0;

	private OctreeNode curNode = null;

	/**
	 * 
	 * @param reader
	 * @param ts
	 * @param te
	 */
	public OctreePostingListIter(DirEntry entry, IBucketBasedSSTableReader reader, int ts, int te) {
		if (entry != null) {
			this.entry = entry;
			this.reader = reader;
			this.ts = ts;
			this.te = te;
			curMin = new Encoding(new Point(0, ts, Integer.MAX_VALUE), 0);
			curMax = new Encoding(new Point(te, Integer.MAX_VALUE, Integer.MAX_VALUE), 0);
			max = new Encoding(new Point(te, Integer.MAX_VALUE, 0), 0);
			nextID.copy(entry.startBucketID);
		}

	}

	@Override
	public PostingListMeta getMeta() {
		return entry;
	}

	@Override
	public void addNode(OctreeNode node) {
		throw new RuntimeException("not supported");
	}

	@Override
	public void open() throws IOException {

	}

	@Override
	public boolean hasNext() throws IOException {
		if (entry != null && curNode == null && diskHasMore()) {
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
		// go to the next layer
		// logger.info("find new code + " + newCode);
		if (newCode.getTopZ() != curMin.getTopZ()) {
			if (curMin.getTopZ() > newCode.getTopZ())
				gotoNewLayer(newCode.getTopZ());
			else
				nextID.offset++;//why?
		} else if (newCode.getX() > te) {
			// 这个走向下一层的标准不对，应该计算当前的面的最大值，只有大于这个最大值时才走向下一个面
			if (newCode.compareTo(curMax) > 0) {
				gotoNewLayer(newCode.getTopZ() - 1);
			} else {
				// 我们需要往回，往上跳,只有公共祖先上侧的点才有可能被访问
				// 先找到公共祖先
				int commBit = ByteUtil.commonBitNum(newCode.getX(), te);
				int newX = ByteUtil.fetchHeadBits(newCode.getX(), commBit);
				int newY = ByteUtil.fetchHeadBits(newCode.getY(), commBit);
				newY |= 1 << (31 - commBit);
				curMin.setPaddingBitNum(0);
				curMin.setX(newX);
				curMin.setY(newY);
				curMin.setTop(newCode.getTopZ());
			}
			nextID.offset++;
		} else if (newCode.getY() + newCode.getEdgeLen() <= ts) {
			nextID.offset++;
			// 先找到公共祖先
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
		} else {
			nextID.offset++;
			return true;
		}
		return false;
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
		Pair<WritableComparableKey, BucketID> pair = null;
		pair = reader.cellOffset(entry.curKey, curMin);
		if (pair == null) {
			pair = reader.floorOffset(entry.curKey, curMin);
		}
		assert pair != null;
		// 利用pair跳转，nextID
		if (curBuck.octNum() == 0 || nextID.compareTo(pair.getValue()) < 0) {
			curBuck.reset();
			nextID.copy(pair.getValue());
		}

		while (diskHasMore()) {
			if (curBuck.octNum() == 0) {
				nextBlockID = reader.getBucket(nextID, curBuck);
			}
			if (nextID.offset < curBuck.octNum()) {
				if (readBucketNextOctant())
					return true;
				else
					return false;
			} else {
				curBuck.reset();
				nextID.blockID = nextBlockID;
				nextID.offset = 0;
			}
		}
		return false;
	}

	private boolean readBucketNextOctant() throws IOException {
		Encoding curCode = new Encoding();
		byte[] data = curBuck.getOctree(nextID.offset);
		DataInputStream input = new DataInputStream(new ByteArrayInputStream(data));
		curCode.read(input);
		// logger.info(curCode);

		if (isCorrectCode(curCode)) {
			curNode = new OctreeNode(curCode, curCode.getEdgeLen());
			curNode.read(input);
			// logger.info("satisfied block " + nextID + " " + curCode);
			return true;
		} else {
			return false;
		}

	}

	private boolean diskHasMore() {
		return curMin.compareTo(max) <= 0 && nextID.compareTo(entry.endBucketID) <= 0;
	}

	private void advance() throws IOException {
		boolean skipping = false;
		while (curNode == null && diskHasMore()) {
			if (!skipping) {
				if (curBuck.octNum() == 0 || nextID.offset < curBuck.octNum()) {
					if (readBucketNextOctant())
						break;
				} else if (curBuck.octNum() != 0) {
					nextID.blockID = nextBlockID;
					nextID.offset = 0;
					nextBlockID = reader.getBucket(nextID, curBuck);
					continue;
				}
			}
			if (skipToOctant()) {
				skipping = false;
				break;
			} else {
				skipping = true;
			}
		}
	}

	@Override
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

}
