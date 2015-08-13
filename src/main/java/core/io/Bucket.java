package core.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.output.ByteArrayOutputStream;

import fanxia.file.ByteUtil;

/**
 * a bucket may correspond to multiple blocks and multiple coctant it is an
 * outputstream like abstraction - total number of octants - octant - octant -
 * padding
 * 
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
	int totalSize = 5;
	List<byte[]> octants = new ArrayList<byte[]>();

	public Bucket(long offset) {
		this.blockIdx = (int) (offset / BLOCK_SIZE);
	}

	public void storeOctant(byte[] octant) {
		octants.add(octant);
		if (octant.length + totalSize > BLOCK_SIZE) {
			singleBlock = false;
			totalSize += 4;
		}
		totalSize += octant.length;
	}

	public void write(DataOutput output) throws IOException {
		ByteArrayOutputStream bOutput = new ByteArrayOutputStream();
		DataOutputStream dos = new DataOutputStream(bOutput);
		dos.writeBoolean(singleBlock);
		// if (!singleBlock) {
		dos.writeInt(octants.size());
		// }
		for (byte[] data : octants) {
			dos.writeInt(data.length);
			dos.write(data);
		}
		dos.close();
		byte[] data = bOutput.toByteArray();
		output.write(data);
		// write the padding bytes

		int occupy = data.length % BLOCK_SIZE;
		if (occupy > 0) {
			byte[] padding = new byte[BLOCK_SIZE - occupy];
			output.write(padding);
		}
	}

	public void read(DataInput input) throws IOException {
		singleBlock = input.readBoolean();
		int num = 1;
		// if (!singleBlock) {
		num = input.readInt();
		// }
		int size = 0;
		octants.clear();
		for (int i = 0; i < num; i++) {
			size = input.readInt();
			totalSize += size + 4;
			byte[] data = new byte[size];
			input.readFully(data);
			octants.add(data);
		}
		int occupy = totalSize % BLOCK_SIZE;
		if (occupy > 0) {
			byte[] padding = new byte[BLOCK_SIZE - occupy];
			input.readFully(padding);
		}
	}

	public static class BucketID implements Comparable<BucketID> {
		public int blockID;
		public short offset;

		public BucketID(int blockID, short offset) {
			this.blockID = blockID;
			this.offset = offset;
		}

		public BucketID(BucketID other) {
			this.blockID = other.blockID;
			this.offset = other.offset;
		}

		public BucketID() {
		}

		public void write(DataOutput output) throws IOException {
			// output.writeInt(blockID);
			ByteUtil.writeVInt(output, blockID);
			ByteUtil.writeVInt(output, offset);
			// output.writeShort(offset);
		}

		public void read(DataInput dis) throws IOException {
			// blockID = dis.readInt();
			blockID = ByteUtil.readVInt(dis);
			offset = (short) ByteUtil.readVInt(dis);
		}

		public long getFileOffset() {
			return blockID * BLOCK_SIZE;
		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see java.lang.Object#toString()
		 */
		@Override
		public String toString() {
			return "BucketID [blockID=" + blockID + ", offset=" + offset + "]";
		}

		@Override
		public int compareTo(BucketID o) {
			int ret = Integer.compare(blockID, o.blockID);
			if (ret == 0) {
				ret = Integer.compare(offset, offset);
			}
			return ret;
		}
	}

	public int blockNum() {
		return (totalSize + BLOCK_SIZE - 1) / BLOCK_SIZE;
	}

	public BucketID blockIdx() {
		return new BucketID(blockIdx, (short) octants.size());
	}

	public boolean canStore(int length) {
		if (singleBlock && octants.isEmpty()) {
			return true;
		} else if (!singleBlock) {
			return false;
		} else if (totalSize + length + 4 <= BLOCK_SIZE) {
			return true;
		}
		return false;
	}

	public byte[] getOctree(int i) {
		assert i < octants.size();
		return octants.get(i);
	}

	public int octNum() {
		return octants.size();
	}

	public void reset() {
		blockIdx = 0;
		singleBlock = true;
		totalSize = 5;
		octants.clear();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "Bucket [blockIdx=" + blockIdx + ", singleBlock=" + singleBlock
				+ ", totalSize=" + totalSize + ", octants=" + octants + "]";
	}

	public void setBlockIdx(int blockIdx) {
		this.blockIdx = blockIdx;
	}

}
