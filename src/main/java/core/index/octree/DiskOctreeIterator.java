package core.index.octree;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Comparator;
import java.util.PriorityQueue;

import core.commom.Encoding;
import core.index.octree.MemoryOctree.OctreeMeta;
import core.io.Bucket;

/**
 * an iterator visiting leaf nodes of disk octree
 * @author xiafan
 *
 */
public class DiskOctreeIterator implements IOctreeIterator {
	File dir;
	OctreeMeta meta;
	PriorityQueue<OctreeNode> traverseQueue = new PriorityQueue<OctreeNode>(
			256, new Comparator<OctreeNode>() {
				@Override
				public int compare(OctreeNode o1, OctreeNode o2) {
					return o1.getEncoding().compareTo(o2.getEncoding());
				}
			});

	DataInputStream dis;
	FileInputStream fis;
	Bucket bucket = null;
	int i = 0;

	/**
	 * 
	 * @param dir the directory where this octree is placed
	 * @param meta the meta data of the octree
	 */
	public DiskOctreeIterator(File dir, OctreeMeta meta) {
		this.dir = dir;
		this.meta = meta;
	}

	public void open() throws FileNotFoundException {
		File dataFile = OctreeZOrderBinaryWriter.octFile(dir, meta);
		fis = new FileInputStream(dataFile);
		dis = new DataInputStream(fis);
	}

	@Override
	public boolean hasNext() throws IOException {
		return !traverseQueue.isEmpty() || dis.available() > 0;
	}

	@Override
	public OctreeNode next() throws IOException {
		if (bucket == null) {
			bucket = new Bucket(fis.getChannel().position());
			bucket.read(dis);
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
		return ret;
	}

	@Override
	public void addNode(OctreeNode node) {
		traverseQueue.add(node);
	}

	@Override
	public void close() {
		meta.ref.decrementAndGet();
	}
}
