package core.index.octree;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import core.index.SSTableWriter;
import core.io.Bucket;

/**
 * This class is only responsible for writing the octree into disk file,
 * not including moving temp file to regular file
 * @author xiafan
 *
 */
public class OctreeZOrderBinaryWriter {
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
			if (octreeNode.size() > 0) {
				ByteArrayOutputStream baos = new ByteArrayOutputStream();
				DataOutputStream dos = new DataOutputStream(baos);
				// first write the block position, then write the octant
				cur.blockIdx().write(dos);
				octreeNode.serialize(dos);
				byte[] data = baos.toByteArray();
				if (cur == null || !cur.canStore(data.length)) {
					if (cur != null)
						cur.write(writer.getDataDos());
					cur = new Bucket(writer.getDataFilePosition());
				}
				if (count++ % step == 0) {
					writer.addSample(octreeNode.getEncoding(), cur.blockIdx());
				}
				cur.storeOctant(data);
			}
		}
	}

	public void close() throws IOException {
		if (cur != null) {
			cur.write(writer.getDataDos());
		}
	}
}
