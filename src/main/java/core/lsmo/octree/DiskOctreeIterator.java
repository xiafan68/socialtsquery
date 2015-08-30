package core.lsmo.octree;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

import Util.Pair;

import common.MidSegment;

import core.commom.Encoding;
import core.io.Bucket;
import core.io.Bucket.BucketID;
import core.lsmo.DiskSSTableReader;
import core.lsmt.ISSTableWriter.DirEntry;
import core.lsmt.IndexKey;
import core.lsmt.PostingListMeta;

/**
 * an iterator visiting leaf nodes of disk octree
 * 
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
			nextBucketID.copy(entry.startBucketID);
		}
	}

	@Override
	public boolean hasNext() throws IOException {
		return entry != null
				&& (!traverseQueue.isEmpty() || nextBucketID
						.compareTo(entry.endBucketID) <= 0);
	}

	@Override
	public OctreeNode nextNode() throws IOException {
		assert nextBucketID.compareTo(entry.endBucketID) <= 0;
		if (bucket.octNum() == 0 || nextBucketID.offset >= bucket.octNum()) {
			bucket.reset();
			nextBucketID.blockID = nextBlockID;
			if (bucket.octNum() != 0)
				nextBucketID.offset = 0;
			bucket.setBlockIdx(nextBlockID);
			nextBlockID = reader.getBucket(nextBucketID, bucket);
		}
		OctreeNode ret = null;
		if (nextBucketID.offset < bucket.octNum()) {
			DataInputStream dis = new DataInputStream(new ByteArrayInputStream(
					bucket.getOctree(nextBucketID.offset)));
			Encoding coding = new Encoding();
			coding.read(dis);
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
		nextBucketID.offset++;

		if (ret.getEncoding().getX() == 699344
				&& ret.getEncoding().getY() == 699344
				&& ret.getEncoding().getZ() == 0 && ret.getEdgeLen() == 16) {
			System.out.println("debuging at next of DiskOctreeIterator "
					+ entry + "  " + nextBucketID + " " + nextBucketID.offset);
		}

		return ret;
	}

	@Override
	public void addNode(OctreeNode node) {
		if (node.getEncoding().getX() == 699344
				&& node.getEncoding().getY() == 699344
				&& node.getEncoding().getZ() == 1 && node.getEdgeLen() == 1) {
			System.out.println("debuging at addNode of DiskOctreeIterator");
		}
		traverseQueue.add(node);
	}

	@Override
	public void close() {

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
		Pair<Integer, List<MidSegment>> ret = new Pair<Integer, List<MidSegment>>(
				node.getEncoding().getTopZ(), new ArrayList<MidSegment>(
						node.segs));
		return ret;
	}

	@Override
	public void skipTo(IndexKey key) throws IOException {
		// TODO Auto-generated method stub

	}
}
