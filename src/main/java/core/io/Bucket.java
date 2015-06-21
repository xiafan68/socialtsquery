package core.io;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * a bucket may correspond to multiple blocks and multiple coctant
 * it is an outputstream like abstraction
 * - total number of octants
 * - octant
 * - octant
 * - padding
 * @author xiafan
 *
 */
public class Bucket {
	public static final int BLOCK_SIZE = 1024 * 4;
	public static final int META_BLOCK = 1;
	public static final int DATA_BLOCK = 2;
	public static final int HEADER_BLOCK = 4;

	int blockIdx = 0;
	boolean singleBlock = true;
	int totalSize = 0;
	List<byte[]> octants = new ArrayList<byte[]>();

	public Bucket(long offset) {
		this.blockIdx = (int) (offset / BLOCK_SIZE);
	}

	public void storeOctant(byte[] octant) {
		octants.add(octant);
		if (octant.length + totalSize > BLOCK_SIZE) {
			singleBlock = false;
		}
		totalSize += octant.length;
	}

	public void write(DataOutput output) throws IOException {
		output.writeBoolean(singleBlock);
		if (!singleBlock) {
			output.writeInt(octants.size());
		}
		for (byte[] data : octants) {
			output.writeInt(data.length);
			output.write(data);
		}
	}

	public void read(DataInput input) throws IOException {
		singleBlock = input.readBoolean();
		int num = 1;
		if (!singleBlock) {
			num = input.readInt();
		}
		int size = 0;
		for (int i = 0; i < num; i++) {
			size = input.readInt();
			byte[] data = new byte[size];
			input.readFully(data);
		}
	}

	public static class BucketID {
		int blockID;
		short offset;

		public BucketID(int blockID, short offset) {
			this.blockID = blockID;
			this.offset = offset;
		}

		public void write(DataOutputStream output) throws IOException {
			output.writeInt(blockID);
			output.writeShort(offset);
		}

		public void read(DataInputStream dis) throws IOException {
			blockID = dis.readInt();
			offset = dis.readShort();
		}

		/* (non-Javadoc)
		 * @see java.lang.Object#toString()
		 */
		@Override
		public String toString() {
			return "BucketID [blockID=" + blockID + ", offset=" + offset + "]";
		}

	}

	public BucketID blockIdx() {
		return new BucketID(blockIdx, (short) octants.size());
	}

	public boolean canStore(int length) {
		if (singleBlock && octants.isEmpty()) {
			return true;
		} else if (!singleBlock) {
			return false;
		} else if (totalSize + length <= BLOCK_SIZE) {
			return true;
		}
		return false;
	}

	public byte[] getOctree(int i) {
		assert i < octants.size();
		return octants.get(i);
	}
}
