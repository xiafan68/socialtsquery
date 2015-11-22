package core.lsmo.internformat;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

import common.MidSegment;
import core.commom.Encoding;
import core.io.Block;
import core.io.Bucket;
import core.io.Bucket.BucketID;
import core.lsmo.octree.IOctreeIterator;
import core.lsmo.octree.OctreeNode;
import core.lsmt.ISSTableWriter.DirEntry;
import core.lsmt.PostingListMeta;
import core.lsmt.WritableComparableKey;
import util.Pair;

/**
 * 用于扫描一个sstable文件中的一个posting list
 * 
 * @author xiafan
 *
 */
public class InternDiskOctreeIterator implements IOctreeIterator {
	DirEntry entry;
	int segNum = 0;

	BucketID nextID = new BucketID(0, (short) 0); // 下一个需要读取的octant
	private Bucket curBuck = new Bucket(Integer.MIN_VALUE); // 当前这个bucket
	private int nextBlockID = 0; // 下一个bucket的blockid

	private OctreeNode curNode = null;
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
	public InternDiskOctreeIterator(DirEntry entry, BlockBasedSSTableReader reader) {
		if (entry != null) {
			this.entry = entry;
			this.reader = reader;
			nextID.copy(entry.startBucketID);
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
		curNode = new OctreeNode(curCode, curCode.getEdgeLen());
		curNode.read(input);
		if (curNode != null)
			segNum += curNode.getSegs().size();
		input.close();
		input = null;
	}

	/**
	 * 判断是否还有必要读取
	 * 
	 * @return
	 */
	private boolean diskHasMore() {
		return nextID.compareTo(entry.endBucketID) <= 0 || !traverseQueue.isEmpty();
	}

	private void advance() throws IOException {
		while (curNode == null && diskHasMore()) {
			if (nextID.compareTo(entry.endBucketID) <= 0) {
				Encoding curCode = readNextOctantCode();
				readNextOctant(curCode);
				nextID.offset++;
			}
			if (curNode == null) {
				curNode = traverseQueue.poll();
			} else if (!traverseQueue.isEmpty()
					&& curNode.getEncoding().compareTo(traverseQueue.peek().getEncoding()) > 0) {
				traverseQueue.offer(curNode);
				curNode = traverseQueue.poll();
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
		if (entry != null && curNode == null && diskHasMore()) {
			advance();
		}
		if (curNode == null) {
			if (entry.size != segNum) {
				System.out.println(entry.curKey);
			}
			assert entry.size == segNum;
		}
		return curNode != null;
	}
}
