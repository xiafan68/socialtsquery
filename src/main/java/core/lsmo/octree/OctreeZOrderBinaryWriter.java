package core.lsmo.octree;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.log4j.Logger;

import core.io.Bucket;
import core.lsmo.OctreeSSTableWriter;

/**
 * This class is only responsible for writing the octree into disk file,
 * not including moving temp file to regular file
 * @author xiafan
 *
 */
public class OctreeZOrderBinaryWriter {
	private static final Logger logger = Logger
			.getLogger(OctreeZOrderBinaryWriter.class);

	Bucket cur; // the current bucket used to write data
	IOctreeIterator iter;
	OctreeSSTableWriter writer;
	int step;

	public OctreeZOrderBinaryWriter(OctreeSSTableWriter writer, IOctreeIterator iter,
			int step) {
		this.writer = writer;
		this.iter = iter;
		this.step = step;
		cur = writer.getBucket();
	}

	/**
	 * the visiting of the parent controls the setup of bucket
	 * @param octreeNode
	 * @throws IOException 
	 */
	public void write() throws IOException {
		OctreeNode octreeNode = null;
		int count = 0;
		while (iter.hasNext()) {
			octreeNode = iter.nextNode();
			if (octreeNode.size() > 0
					|| OctreeNode.isMarkupNode(octreeNode.getEncoding())) {
				int[] counters = octreeNode.histogram();
				if (octreeNode.getEdgeLen() != 1
						&& counters[0] > (MemoryOctree.size_threshold >> 1)) {
					octreeNode.split();
					for (int i = 0; i < 8; i++)
						iter.addNode(octreeNode.getChild(i));
				} else {
					ByteArrayOutputStream baos = new ByteArrayOutputStream();
					DataOutputStream dos = new DataOutputStream(baos);
					// first write the octant code, then write the octant
					octreeNode.getEncoding().write(dos);
					octreeNode.write(dos);
					byte[] data = baos.toByteArray();
					if (!cur.canStore(data.length)) {
						cur.write(writer.getDataDos());
						logger.debug(cur);
						cur = writer.newBucket();
					}
					if (count++ % step == 0) {
						writer.addSample(octreeNode.getEncoding(),
								cur.blockIdx());
					}
					logger.debug(octreeNode);
					cur.storeOctant(data);
				}
			}
		}
	}

	public void close() throws IOException {
	}
}
