package core.index.octree;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.Comparator;
import java.util.PriorityQueue;

import core.commom.Encoding;
import core.index.DiskSSTableReader;
import core.index.SSTableWriter.DirEntry;
import core.index.octree.MemoryOctree.OctreeMeta;
import core.io.Bucket;
import core.io.Bucket.BucketID;

/**
 * an iterator visiting leaf nodes of disk octree
 * @author xiafan
 *
 */
public class DiskOctreeIterator implements IOctreeIterator {
	DirEntry entry;
	PriorityQueue<OctreeNode> traverseQueue = new PriorityQueue<OctreeNode>(
			256, new Comparator<OctreeNode>() {
				@Override
				public int compare(OctreeNode o1, OctreeNode o2) {
					return o1.getEncoding().compareTo(o2.getEncoding());
				}
			});

	Bucket bucket = new Bucket(-1);
	BucketID nextBucketID = new BucketID(0, (short) 0);

	int curIdx = 0;
	int readNum = 0;

	private DiskSSTableReader reader;

	/**
	 * 
	 * @param dir the directory where this octree is placed
	 * @param meta the meta data of the octree
	 */
	public DiskOctreeIterator(DirEntry entry, DiskSSTableReader reader) {
		this.entry = entry;
		this.reader = reader;
		nextBucketID.blockID = entry.dataStartBlockID;
	}

	@Override
	public boolean hasNext() throws IOException {
		return !traverseQueue.isEmpty() || readNum < entry.dataBlockNum
				|| curIdx < bucket.octNum();
	}

	@Override
	public OctreeNode next() throws IOException {
		if (readNum < entry.dataBlockNum && curIdx >= bucket.octNum()) {
			bucket.reset();
			bucket.setBlockIdx(nextBucketID.blockID);
			nextBucketID.blockID = reader.getBucket(nextBucketID, bucket);
			readNum += bucket.blockNum();
			curIdx = 0;
		}
		OctreeNode ret = null;
		if (curIdx < bucket.octNum()) {
			DataInputStream dis = new DataInputStream(new ByteArrayInputStream(
					bucket.getOctree(curIdx)));
			Encoding coding = new Encoding();
			coding.readFields(dis);
			ret = new OctreeNode(coding, coding.getEdgeLen());
			ret.read(dis);
		}

		if (ret == null
				|| (!traverseQueue.isEmpty() && ret.getEncoding().compareTo(
						traverseQueue.peek().getEncoding()) > 0)) {
			if (ret != null)
				traverseQueue.offer(ret);
			ret = traverseQueue.poll();
		}
		curIdx++;
		return ret;
	}

	@Override
	public void addNode(OctreeNode node) {
		traverseQueue.add(node);
	}

	@Override
	public void close() {

	}

	@Override
	public OctreeMeta getMeta() {
		return entry;
	}

	@Override
	public void open() throws IOException {

	}
}
