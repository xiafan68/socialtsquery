package core.lsmo.octree;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.log4j.Logger;

import core.io.Bucket;
import core.lsmo.SSTableWriter;

/**
 * This class is only responsible for writing the octree into disk file, not
 * including moving temp file to regular file
 * 
 * @author xiafan
 *
 */
public class OctreeZOrderBinaryWriter {
	private static final Logger logger = Logger.getLogger(OctreeZOrderBinaryWriter.class);

	Bucket cur; // the current bucket used to write data
	IOctreeIterator iter;
	SSTableWriter writer;
	int step;

	public OctreeZOrderBinaryWriter(SSTableWriter writer, IOctreeIterator iter, int step) {
		this.writer = writer;
		this.iter = iter;
		this.step = step;
		cur = writer.getBucket();
	}

	

	public void close() throws IOException {
	}
}
