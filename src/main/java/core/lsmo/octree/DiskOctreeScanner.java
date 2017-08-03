package core.lsmo.octree;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

import common.MidSegment;
import core.commom.Encoding;
import core.io.Bucket;
import core.io.Bucket.BucketID;
import core.lsmt.IBucketBasedSSTableReader;
import core.lsmt.ISSTableWriter.DirEntry;
import util.Pair;
import core.lsmt.PostingListMeta;
import core.lsmt.WritableComparableKey;

/**
 * 用于扫描一个sstable文件中的一个posting list
 * 
 * @author xiafan
 *
 */
public class DiskOctreeScanner implements IOctreeIterator {
	DirEntry entry;
	PriorityQueue<OctreeNode> traverseQueue = new PriorityQueue<OctreeNode>(256, new Comparator<OctreeNode>() {
		@Override
		public int compare(OctreeNode o1, OctreeNode o2) {
			return o1.getEncoding().compareTo(o2.getEncoding());
		}
	});

	Bucket bucket = new Bucket(-1);
	BucketID nextBucketID = new BucketID(0, (short) 0);

	int nextBlockID = -1;
	private IBucketBasedSSTableReader reader;

	/**
	 * 
	 * @param dir
	 *            the directory where this octree is placed
	 * @param meta
	 *            the meta data of the octree
	 */
	public DiskOctreeScanner(DirEntry entry, IBucketBasedSSTableReader reader) {
		if (entry != null) {
			this.entry = entry;
			this.reader = reader;
			nextBucketID.copy(entry.startBucketID);
		}
	}

	@Override
	public boolean hasNext() throws IOException {
		return entry != null && (!traverseQueue.isEmpty() || nextBucketID.compareTo(entry.endBucketID) <= 0);
	}

	@Override
	public OctreeNode nextNode() throws IOException {
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
			coding.read(dis);
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
				new ArrayList<MidSegment>(node.segs));
		return ret;
	}

	@Override
	public void skipTo(WritableComparableKey key) throws IOException {
		// TODO Auto-generated method stub

	}
}
