package core.index;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;

import Util.Pair;
import core.commom.Encoding;
import core.commom.Point;
import core.index.SSTableWriter.DirEntry;
import core.index.octree.IOctreeIterator;
import core.index.octree.MemoryOctree.OctreeMeta;
import core.index.octree.OctreeNode;
import core.io.Bucket;
import core.io.Bucket.BucketID;
import fanxia.file.ByteUtil;

/**
 * 用于访问disk上的octree
 * @author xiafan
 *
 */
public class OctreePostingListIter implements IOctreeIterator {
	private DirEntry entry;
	private DiskSSTableReader reader;
	private int ts;
	private int te;

	private Encoding curMin = null;
	private Encoding max = null;
	private BucketID nextID = new BucketID(0, (short) 0);
	private Bucket curBuck = new Bucket(0);
	private OctreeNode curNode = null;
	private int nextBlockID = 0;

	/**
	 * 
	 * @param reader
	 * @param ts
	 * @param te
	 */
	public OctreePostingListIter(DirEntry entry, DiskSSTableReader reader,
			int ts, int te) {
		this.entry = entry;
		this.reader = reader;
		this.ts = ts;
		this.te = te;
		curMin = new Encoding(new Point(ts, ts, Integer.MAX_VALUE), 0);
		max = new Encoding(new Point(te, Integer.MAX_VALUE, 0), 0);
	}

	@Override
	public OctreeMeta getMeta() {
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
		return curNode != null || diskHasMore();
	}

	private void gotoNewLayer(int layer) {
		curMin.setX(ts);
		curMin.setY(te);
		curMin.setZ(layer);
	}

	private boolean isCorrectCode(Encoding newCode) {
		// go to the next layer
		if (newCode.getZ() != curMin.getZ()) {
			gotoNewLayer(newCode.getZ());
		} else if (newCode.getX() > te) {
			gotoNewLayer(newCode.getZ() - 1);
		} else if (newCode.getY() < ts) {
			int commBit = ByteUtil.commonBits(newCode.getY(), ts);
			int newY = ByteUtil.commonBits(newCode.getY(), commBit);
			int mask = 1 << (31 - commBit);
			newY |= mask;
			int newZ = ByteUtil.commonBits(newCode.getX(), commBit);
			curMin.setY(newY);
			curMin.setZ(newZ);
		} else {
			return true;
		}
		return false;
	}

	/**
	 * 开始的时候，用curMin去找到第一个bucket
	 * 
	 * 一直往前跑，
	 * 如果当前跑到查询区间下面去了，生成新的code，跳转
	 * 如果当前跑到查询区间的外面去了，生成新的code，跳转
	 * 
	 */

	/**
	 *  在index上找到第一个大于等于参数的octant
	 * @throws IOException 
	 */
	private boolean locateOctant() throws IOException {
		Pair<Encoding, BucketID> pair = null;
		pair = reader.cellOffset(entry.curKey, curMin);
		if (pair == null) {
			pair = reader.floorOffset(entry.curKey, curMin);
		}
		assert pair != null;
		// 利用pair跳转，nextID
		if (curBuck.octNum() != 0) {
			if (nextID.compareTo(pair.getValue()) < 0) {
				curBuck.reset();
				nextID = pair.getValue();
			}
		}

		while (nextID.blockID - entry.dataStartBlockID < entry.dataBlockNum) {
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
		nextID.offset++;
		DataInputStream input = new DataInputStream(new ByteArrayInputStream(
				data));
		curCode.readFields(input);
		if (curCode.compareTo(curMin) >= 0) {
			if (isCorrectCode(curCode)) {
				curNode = new OctreeNode(null, 0);
				curNode.setPoint(curCode);
				curNode.read(input);
				return true;
			} else {
				return false;
			}
		}
		return false;
	}

	private boolean diskHasMore() {
		return curMin.compareTo(max) <= 0
				&& (nextID.blockID - entry.dataStartBlockID < entry.dataBlockNum || (curBuck
						.octNum() != 0 && nextID.offset < curBuck.octNum()));
	}

	private void advance() throws IOException {
		boolean skipping = false;
		while (curNode == null && diskHasMore()) {
			if (!skipping) {
				if (curBuck.octNum() != 0 && nextID.offset < curBuck.octNum()) {
					if (readBucketNextOctant())
						break;
					else {
						skipping = true;
					}
				} else if (curBuck.octNum() != 0) {
					nextID.blockID = nextBlockID;
					nextID.offset = 0;
					nextBlockID = reader.getBucket(nextID, curBuck);
					continue;
				}
			}
			if (locateOctant()) {
				skipping = false;
				break;
			} else {
				skipping = true;
			}
		}
	}

	@Override
	public OctreeNode next() throws IOException {
		advance();
		OctreeNode ret = curNode;
		curNode = null;
		return ret;
	}

	@Override
	public void close() throws IOException {
	}
}
