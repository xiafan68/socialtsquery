package core.index.octree;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.Comparator;
import java.util.PriorityQueue;

import core.commom.Encoding;
import core.index.SSTableReader;
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
	Bucket bucket = null;
	int i = 0;
	int readNum = 0;
	private SSTableReader reader;

	/**
	 * 
	 * @param dir the directory where this octree is placed
	 * @param meta the meta data of the octree
	 */
	public DiskOctreeIterator(DirEntry entry, SSTableReader reader) {
		this.entry = entry;
		this.reader = reader;
	}

	@Override
	public boolean hasNext() throws IOException {
		return !traverseQueue.isEmpty() || readNum < entry.dataBlockNum;
	}

	@Override
	public OctreeNode next() throws IOException {
		if (bucket == null || i < bucket.octNum()) {
			bucket = reader.getBucket(new BucketID(entry.dataStartBlockID,
					(short) 0));
			readNum += bucket.blockNum();
			i = 0;
		}
		DataInputStream dis = new DataInputStream(new ByteArrayInputStream(
				bucket.getOctree(i)));
		Encoding coding = new Encoding();
		coding.readFields(dis);
		OctreeNode ret = new OctreeNode(coding, coding.getEdgeLen());
		if (!traverseQueue.isEmpty()
				&& ret.getEncoding().compareTo(
						traverseQueue.peek().getEncoding()) > 0) {
			traverseQueue.offer(ret);
			ret = traverseQueue.poll();
		}
		i++;
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
