package core.index.octree;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

import core.io.Bucket;

public class OctreeZOrderBinaryWriter {
	File dir;
	int version;
	Bucket cur; // the current bucket used to write data
	FileOutputStream fileOs;
	DataOutputStream dataDos;
	DataOutputStream indexDos; // write down the encoding of each block
	IOctreeIterator iter;

	public OctreeZOrderBinaryWriter(File dir, int version, IOctreeIterator iter) {
		this.dir = dir;
		this.version = version;
		this.iter = iter;
	}

	public static File octFile(File dir, int version) {
		return new File(dir, String.format("%d.octs", version));
	}

	public static File idxFile(File dir, int version) {
		return new File(dir, String.format("%d.idx", version));
	}

	public void open() throws FileNotFoundException {
		fileOs = new FileOutputStream(octFile(dir, version));
		dataDos = new DataOutputStream(fileOs);
		indexDos = new DataOutputStream(new FileOutputStream(idxFile(dir,
				version)));
	}

	/**
	 * the visiting of the parent controls the setup of bucket
	 * @param octreeNode
	 * @throws IOException 
	 */
	public void write() {
		OctreeNode octreeNode = null;
		while (iter.hasNext()) {
			octreeNode = iter.next();
			if (octreeNode.size() > 0) {
				try {
					ByteArrayOutputStream baos = new ByteArrayOutputStream();
					DataOutputStream dos = new DataOutputStream(baos);
					octreeNode.serialize(dos);
					byte[] data = baos.toByteArray();
					if (cur == null || !cur.canStore(data.length)) {
						if (cur != null)
							cur.write(dataDos);
						cur = new Bucket(fileOs.getChannel().position());
					}

					octreeNode.getEncoding().write(indexDos);
					indexDos.writeInt(cur.blockIdx());
					cur.storeOctant(data);
				} catch (IOException e) {
					e.printStackTrace();
				}
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
