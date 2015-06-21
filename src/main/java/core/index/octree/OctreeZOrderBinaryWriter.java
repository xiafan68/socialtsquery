package core.index.octree;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

import Util.Configuration;
import core.index.LogStructureOctree.OctreeMeta;
import core.io.Bucket;

/**
 * This class is only responsible for writing the octree into disk file,
 * not including moving temp file to regular file
 * @author xiafan
 *
 */
public class OctreeZOrderBinaryWriter {
	Configuration conf;
	OctreeMeta meta;
	Bucket cur; // the current bucket used to write data
	FileOutputStream fileOs;
	DataOutputStream dataDos;
	DataOutputStream indexDos; // write down the encoding of each block
	IOctreeIterator iter;

	public OctreeZOrderBinaryWriter(Configuration conf, OctreeMeta meta,
			IOctreeIterator iter) {
		this.conf = conf;
		this.meta = meta;
		this.iter = iter;
	}

	public static File octFile(File dir, OctreeMeta meta) {
		return new File(dir, String.format("%d_%d.octs", meta.version,
				meta.fileSeq));
	}

	public static File idxFile(File dir, OctreeMeta meta) {
		return new File(dir, String.format("%d_%d.idx", meta.version,
				meta.fileSeq));
	}

	public void open() throws FileNotFoundException {
		fileOs = new FileOutputStream(octFile(conf.getTmpDir(), meta));
		dataDos = new DataOutputStream(fileOs);
		indexDos = new DataOutputStream(new FileOutputStream(idxFile(
				conf.getTmpDir(), meta)));
	}

	/**
	 * the visiting of the parent controls the setup of bucket
	 * @param octreeNode
	 * @throws IOException 
	 */
	public void write() throws IOException {
		OctreeNode octreeNode = null;
		int step = conf.getIndexStep();
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
						cur.write(dataDos);
					cur = new Bucket(fileOs.getChannel().position());
				}
				if (count++ % step == 0) {
					octreeNode.getEncoding().write(indexDos);
					cur.blockIdx().write(indexDos);
				}
				cur.storeOctant(data);
			}
		}
	}

	public void close() throws IOException {
		if (cur != null) {
			cur.write(dataDos);
		}
		dataDos.close();
		fileOs.close();
		indexDos.close();
	}
}
