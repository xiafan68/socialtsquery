package core.lsmo.octree;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

import core.io.Bucket;

public class OctreeTextWriter implements OctreeVisitor {
	File dir;
	int version;
	Bucket cur; // the current bucket used to write data
	int blockIdx;

	public OctreeTextWriter(File dir, int version) {
		this.dir = dir;
		this.version = version;
	}

	public static File octFile(File dir, int version) {
		return new File(dir, String.format("%d.octs", version));
	}

	public static File idxFile(File dir, int version) {
		return new File(dir, String.format("%d.idx", version));
	}

	public void open() throws FileNotFoundException {
	}

	public void visitIntern(OctreeNode octreeNode) {
		// first visit the upper octants
		for (int i = 4; i < 8; i++) {
			octreeNode.getChild(i).visit(this);
		}

		for (int i = 0; i < 4; i++) {
			octreeNode.getChild(i).visit(this);
		}
	}

	/**
	 * the visiting of the parent controls the setup of bucket
	 * @param octreeNode
	 * @throws IOException 
	 */
	public void visitLeaf(OctreeNode octreeNode) {
		if (octreeNode.size() > 0) {
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			DataOutputStream dos = new DataOutputStream(baos);
			octreeNode.write(dos);
			byte[] data = baos.toByteArray();
			if (cur == null || !cur.canStore(data.length)) {
				cur = new Bucket(blockIdx * Bucket.BLOCK_SIZE);
			}

			System.out.println(String.format("leaf node: %s, id:%s;",
					octreeNode.getEncoding(), cur.blockIdx().toString()));
			cur.storeOctant(data);
		}
	}
}
