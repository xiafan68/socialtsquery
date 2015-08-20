package core.lsmo.octree;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.Comparator;
import java.util.PriorityQueue;

import core.commom.Encoding;
import core.io.Bucket;
import core.io.Bucket.BucketID;
import core.lsmo.DiskSSTableReader;
import core.lsmo.SSTableWriter.DirEntry;
import core.lsmo.octree.MemoryOctree.OctreeMeta;

/**
 * an iterator visiting leaf nodes of disk octree
 * 
 * @author xiafan
 *
 */
public class DiskOctreeIterator implements IOctreeIterator {
	DirEntry entry;
	PriorityQueue<OctreeNode> traverseQueue = new PriorityQueue<OctreeNode>(256, new Comparator<OctreeNode>() {
		@Override
		public int compare(OctreeNode o1, OctreeNode o2) {
			return o1.getEncoding().compareTo(o2.getEncoding());
		}
	});

	Bucket bucket = new Bucket(-1);
	BucketID nextBucketID;

	int nextBlockID = -1;
	private DiskSSTableReader reader;

	/**
	 * 
	 * @param dir
	 *            the directory where this octree is placed
	 * @param meta
	 *            the meta data of the octree
	 */
	public DiskOctreeIterator(DirEntry entry, DiskSSTableReader reader) {
		if (entry != null) {
			this.entry = entry;
			this.reader = reader;
			nextBucketID = new BucketID(entry.startBucketID);
		}
	}

	@Override
	public boolean hasNext() throws IOException {
		return entry != null && (!traverseQueue.isEmpty() || nextBucketID.compareTo(entry.endBucketID) <= 0);
	}

	@Override
	public OctreeNode next() throws IOException {
		// load new bucket
		OctreeNode ret = null;
		if (nextBucketID.compareTo(entry.endBucketID) <= 0) {
			if (bucket.octNum() == 0 || nextBucketID.offset >= bucket.octNum()) {
				if (bucket.octNum() != 0) {
					nextBucketID.offset = 0;
					nextBucketID.blockID = nextBlockID;
				}
				bucket.reset();
				bucket.setBlockIdx(nextBucketID.blockID);
				nextBlockID = reader.getBucket(nextBucketID, bucket);
			}

			assert nextBucketID.offset < bucket.octNum();
			DataInputStream dis = new DataInputStream(new ByteArrayInputStream(bucket.getOctree(nextBucketID.offset)));
			Encoding coding = new Encoding();
			coding.readFields(dis);
			ret = new OctreeNode(coding, coding.getEdgeLen());
			ret.read(dis);
			nextBucketID.offset++;
		}

		if (ret == null
				|| (!traverseQueue.isEmpty() && ret.getEncoding().compareTo(traverseQueue.peek().getEncoding()) > 0)) {
			if (ret != null)
				traverseQueue.offer(ret);
			ret = traverseQueue.poll();
		}

		if (ret.getEncoding().getX() == 655360 && ret.getEncoding().getY() == 655360 && ret.getEncoding().getZ() == 4096
				&& ret.getEdgeLen() == (1 << 12)) {
			System.out.println("debuging at next of DiskOctreeIterator " + entry + "  " + nextBucketID + " "
					+ nextBucketID.offset);
		}

		return ret;
	}

	@Override
	public void addNode(OctreeNode node) {
		if (node.getEncoding().getX() == 699344 && node.getEncoding().getY() == 699344 && node.getEncoding().getZ() == 1
				&& node.getEdgeLen() == 1) {
			System.out.println("debuging at addNode of DiskOctreeIterator");
		}
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
