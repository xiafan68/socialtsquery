package core.lsmo.octree;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import common.MidSegment;
import core.commom.Encoding;
import core.commom.Point;
import core.io.Bucket;
import core.io.Bucket.BucketID;
import core.lsmt.IBucketBasedSSTableReader;
import core.lsmt.ISSTableWriter.DirEntry;
import core.lsmt.PostingListMeta;
import core.lsmt.WritableComparable;
import io.ByteUtil;
import util.Pair;
import util.Profile;
import util.ProfileField;

/**
 * 用于访问disk上的octree
 * 
 * 1. 最有当前node的code大于当前的curMax当前层的访问才结束，而不是当code.x大于te时，当code.x大于te时，
 * 之后访问的节点还是可能绕会te.
 * 
 * @author xiafan
 *
 */
public class OctreePostingListIter implements IOctreeIterator {
	private static final Logger logger = Logger.getLogger(OctreePostingListIter.class);
	protected DirEntry entry;
	protected IBucketBasedSSTableReader reader;
	protected int ts;
	protected int te;

	protected Encoding curMin = null;
	protected Encoding curMax = null;
	protected Encoding max = null;

	protected BucketID nextID = new BucketID(0, (short) 0); // 下一个需要读取的octant
	protected Bucket curBuck = new Bucket(Integer.MIN_VALUE); // 当前这个bucket
	protected int nextBlockID = 0; // 下一个bucket的blockid

	protected OctreeNode curNode = null;
	protected DataInputStream input = null;

	/**
	 * 
	 * @param reader
	 * @param ts
	 * @param te
	 */
	public OctreePostingListIter(DirEntry entry, IBucketBasedSSTableReader reader, int ts, int te) {
		if (entry != null && entry.curKey != null && entry.minTime <= te && entry.maxTime >= ts) {
			Profile.instance.updateCounter(ProfileField.HITTED_LEVEL_NUM.toString(), 1);
			Profile.instance.updateCounter(ProfileField.HITTED_LEVELS.toString(), reader.getMeta().level);

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
		if (entry != null && entry.curKey != null && curNode == null && diskHasMore()) {
			advance();
		}
		return curNode != null;
	}

	protected void gotoNewLayer(int layer) {
		curMin.setPaddingBitNum(0);
		curMin.setX(0);
		curMin.setY(ts);
		curMin.setTop(layer);

		curMax.setPaddingBitNum(0);
		curMax.setX(te);
		curMax.setY(Integer.MAX_VALUE);
		curMax.setTop(layer);
	}

	protected boolean isCorrectCode(Encoding newCode) {
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

	protected BucketID cellOffset() throws IOException {
		BucketID pair = null;
		pair = reader.cellOffset(entry.curKey, curMin);
		if (pair == null) {
			pair = reader.floorOffset(entry.curKey, curMin);
		}
		return pair;
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
	protected boolean skipToOctant() throws IOException {
		BucketID id = cellOffset();
		if (id != null) {
			// 利用pair跳转，nextID
			if (curBuck.octNum() == 0 || nextID.compareTo(id) < 0) {
				curBuck.reset();
				nextID.copy(id);
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

	protected void readNextBucket() throws IOException {
		nextBlockID = reader.getBucket(nextID, curBuck);
	}

	/**
	 * 只负责顺序的读取下一个octant
	 * 
	 * @throws IOException
	 */
	protected Encoding readNextOctantCode() throws IOException {
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

	protected void readNextOctant(Encoding curCode) throws IOException {
		curNode = new OctreeNode(curCode, curCode.getEdgeLen());
		curNode.read(input);
		input.close();
		input = null;
	}

	/**
	 * 判断是否还有必要读取
	 * 
	 * @return
	 */
	protected boolean diskHasMore() {
		return curMin.compareTo(max) <= 0 && nextID.compareTo(entry.endBucketID) <= 0;
	}

	/**
	 * 开始的时候，用curMin去找到第一个bucket
	 * 
	 * 一直往前跑， 如果当前跑到查询区间下面去了，生成新的code，跳转 如果当前跑到查询区间的外面去了，生成新的code，跳转
	 * 
	 */
	protected void advance() throws IOException {
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

	@Override
	public OctreeNode nextNode() throws IOException {
		advance();
		OctreeNode ret = curNode;
		logger.debug(entry.toString() + ";" + ret.getEncoding().toString());
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
	public void skipTo(WritableComparable key) throws IOException {
		// TODO Auto-generated method stub

	}

}
