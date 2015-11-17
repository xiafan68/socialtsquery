package core.lsmo.octree;

import java.io.IOException;

import org.apache.log4j.Logger;

import core.io.Bucket;
import core.lsmo.bdbformat.OctreeSSTableWriter;

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
		cur = writer.getDataBucket();
	}


	public void close() throws IOException {
	}
}
