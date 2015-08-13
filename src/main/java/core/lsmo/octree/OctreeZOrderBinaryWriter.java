package core.lsmo.octree;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.log4j.Logger;

import core.io.Bucket;
import core.lsmo.SSTableWriter;

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
	SSTableWriter writer;
	int step;

	public OctreeZOrderBinaryWriter(SSTableWriter writer, IOctreeIterator iter,
			int step) {
		this.writer = writer;
		this.iter = iter;
		this.step = step;
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
			octreeNode = iter.next();
			if (octreeNode.size() > 0
					|| OctreeNode.isMarkupNode(octreeNode.getEncoding())) {
				ByteArrayOutputStream baos = new ByteArrayOutputStream();
				DataOutputStream dos = new DataOutputStream(baos);
				// first write the octant code, then write the octant
				octreeNode.getEncoding().write(dos);
				octreeNode.write(dos);
				byte[] data = baos.toByteArray();
				if (cur == null || !cur.canStore(data.length)) {
					if (cur != null) {
						cur.write(writer.getDataDos());
						logger.debug(cur);
					}
					cur = new Bucket(writer.getDataFilePosition());
				}
				if (count++ % step == 0) {
					writer.addSample(octreeNode.getEncoding(), cur.blockIdx());
				}
				logger.debug(octreeNode);
				cur.storeOctant(data);
			}
		}
	}

	public void close() throws IOException {
		if (cur != null) {
			cur.write(writer.getDataDos());
			logger.debug("last bucket:" + cur.toString());
		}
	}
}
