package core.lsmo.internformat;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

import org.apache.log4j.Logger;

import common.MidSegment;
import core.commom.Encoding;
import core.commom.WritableComparableKey;
import core.io.Block;
import core.io.Bucket;
import core.io.Bucket.BucketID;
import core.lsmo.MarkDirEntry;
import core.lsmo.octree.IOctreeIterator;
import core.lsmo.octree.OctreeNode;
import core.lsmt.DirEntry;
import core.lsmt.postinglist.PostingListMeta;
import util.Pair;

/**
 * 用于扫描一个sstable文件中的一个posting list
 * 
 * @author xiafan
 *
 */
public class InternDiskOctreeScanner implements IOctreeIterator {
	private Logger logger = Logger.getLogger(InternDiskOctreeScanner.class);

	DirEntry entry;
	int segNum = 0;

	BucketID nextID = new BucketID(0, (short) 0); // 下一个需要读取的octant
	private Bucket curBuck = new Bucket(Integer.MIN_VALUE); // 当前这个bucket
	private int nextBlockID = 0; // 下一个bucket的blockid

	BucketID nextMarkID = new BucketID(0, (short) 0); // 下一个需要读取的octant
	private Bucket curMarkBuck = new Bucket(Integer.MIN_VALUE); // 当前这个bucket
	private int nextMarkBlockID = 0; // 下一个bucket的blockid
	private int readMarkNum = 0;

	private OctreeNode[] curNodes = new OctreeNode[] { null, null };

	DataInputStream input = null;
	private BlockBasedSSTableReader reader;

	PriorityQueue<OctreeNode> traverseQueue = new PriorityQueue<OctreeNode>(256, new Comparator<OctreeNode>() {
		@Override
		public int compare(OctreeNode o1, OctreeNode o2) {
			return o1.getEncoding().compareTo(o2.getEncoding());
		}
	});

	/**
	 * 
	 * @param dir
	 *            the directory where this octree is placed
	 * @param meta
	 *            the meta data of the octree
	 */
	public InternDiskOctreeScanner(DirEntry entry, BlockBasedSSTableReader reader) {
		if (entry != null) {
			this.entry = entry;
			this.reader = reader;
			nextID.copy(entry.startBucketID);
			nextMarkID.copy(((MarkDirEntry) entry).startMarkOffset);
		}
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
		curNodes[0] = new OctreeNode(curCode, curCode.getEdgeLen());
		curNodes[0].read(input);
		if (curNodes[0] != null)
			segNum += curNodes[0].getSegs().size();
		input.close();
		input = null;
	}

	/**
	 * 判断是否还有必要读取
	 * 
	 * @return
	 */
	private boolean diskHasMore() {
		return curNodes[0] != null || curNodes[1] != null || nextID.compareTo(entry.endBucketID) <= 0
				|| !traverseQueue.isEmpty() || readMarkNum < ((MarkDirEntry) entry).markNum;
	}

	private void nextMarkNode() throws IOException {
		if (readMarkNum < ((MarkDirEntry) entry).markNum && curNodes[1] == null) {
			if (curMarkBuck.blockIdx().blockID < 0) {
				curMarkBuck.reset();
				curMarkBuck.setBlockIdx(nextMarkID.blockID);
				nextMarkBlockID = reader.getBucketFromMarkFile(curMarkBuck);
			} else if (nextMarkID.offset >= curMarkBuck.octNum()) {
				curMarkBuck.reset();
				nextMarkID.blockID = nextMarkBlockID;
				nextMarkID.offset = 0;
				curMarkBuck.setBlockIdx(nextMarkBlockID);
				nextMarkBlockID = reader.getBucketFromMarkFile(curMarkBuck);
			}
			Encoding curCode = new Encoding();
			byte[] data = curMarkBuck.getOctree(nextMarkID.offset);
			input = new DataInputStream(new ByteArrayInputStream(data));
			curCode.read(input);
			curNodes[1] = new OctreeNode(curCode, curCode.getEdgeLen());
			nextMarkID.offset++;
			readMarkNum++;
		}
	}

	private void nextDataOctant() throws IOException {
		if (curNodes[0] == null && nextID.compareTo(entry.endBucketID) <= 0) {
			Encoding curCode = readNextOctantCode();
			readNextOctant(curCode);
			nextID.offset++;
		}
	}

	private int largeNode() {
		int ret = -1;
		if (curNodes[0] == null && curNodes[1] == null)
			return -1;
		if (curNodes[0] != null && curNodes[1] == null) {
			ret = 0;
		} else if (curNodes[0] == null && curNodes[1] != null) {
			ret = 1;
		} else {
			if (curNodes[0].getEncoding().compareTo(curNodes[1].getEncoding()) <= 0) {
				ret = 0;
			} else {
				ret = 1;
			}
		}
		return ret;
	}

	private OctreeNode advance() throws IOException {
		OctreeNode ret = null;
		if (diskHasMore()) {
			nextDataOctant();
			nextMarkNode();
			int idx = largeNode();
			if (idx == -1) {
				ret = traverseQueue.poll();
			} else {
				if (!traverseQueue.isEmpty()
						&& curNodes[idx].getEncoding().compareTo(traverseQueue.peek().getEncoding()) > 0)
					ret = traverseQueue.poll();
				else {
					ret = curNodes[idx];
					curNodes[idx] = null;
				}
			}
		}
		return ret;
	}

	public OctreeNode nextNode() throws IOException {
		OctreeNode ret = advance();
		return ret;
	}

	@Override
	public void close() throws IOException {
		if (input != null) {
			input.close();
		}
	}

	@Override
	public void addNode(OctreeNode node) {
		traverseQueue.add(node);
	}

	@Override
	public PostingListMeta getMeta() {
		return entry;
	}

	@Override
	public void open() throws IOException {

	}

	@Override
	public Pair<Integer, List<MidSegment>> next() throws IOException {
		OctreeNode node = nextNode();
		Pair<Integer, List<MidSegment>> ret = new Pair<Integer, List<MidSegment>>(node.getEncoding().getTopZ(),
				new ArrayList<MidSegment>(node.getSegs()));
		return ret;
	}

	@Override
	public void skipTo(WritableComparableKey key) throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public boolean hasNext() throws IOException {
		return diskHasMore();
	}
}
