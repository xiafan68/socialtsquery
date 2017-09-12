package core.io;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.io.output.ByteArrayOutputStream;

import core.io.Block.BLOCKTYPE;
import core.lsmt.WritableComparable;
import io.ByteUtil;

/**
 * When storing octants into disk, they are first written into a bucket. A
 * bucket may contain multiple octants and stored in multiple blocks. The
 * storage format of the bucket is: [total number of octants] [octant] [octant]
 * [padding bits]
 * 
 * @author xiafan
 *
 */
public class Bucket {
	boolean singleBlock = true;
	int totalSize = 5;
	List<byte[]> octants = new ArrayList<byte[]>();

	int blockIdx = 0;

	public Bucket(int blockIdx) {
		this.blockIdx = blockIdx;
	}

	public Bucket(long offset) {
		blockIdx = (int) (offset / Block.BLOCK_SIZE);
	}

	public void storeOctant(byte[] octant) {
		octants.add(octant);
		if (octant.length + totalSize + 4 > Block.availableSpace()) {
			singleBlock = false;
		}
		totalSize += 4;
		totalSize += octant.length;
	}

	public List<Block> toBlocks() throws IOException {
		List<Block> ret = new ArrayList<Block>();
		ByteArrayOutputStream bOutput = new ByteArrayOutputStream();
		DataOutputStream dos = new DataOutputStream(bOutput);

		dos.writeInt(octants.size());
		for (byte[] data : octants) {
			dos.writeInt(data.length);
			dos.write(data);
		}
		dos.close();

		byte[] nData = bOutput.toByteArray();
		// System.out.println("original array:\n" + Arrays.toString(nData));
		int start = 0;
		int end = 0;
		Block block = new Block(BLOCKTYPE.DATA_BLOCK, 0);
		ByteArrayOutputStream tempBount = new ByteArrayOutputStream(Block.availableSpace());
		while (start < nData.length) {
			end = start + Block.availableSpace() - 1;
			end = Math.min(end, nData.length);
			if (end >= nData.length) {
				tempBount.write(1);
			} else {
				tempBount.write(0);
			}
			tempBount.write(Arrays.copyOfRange(nData, start, end));
			block.setData(tempBount.toByteArray());
			tempBount.reset();
			ret.add(block);
			block = new Block(BLOCKTYPE.DATA_BLOCK, 0);
			start = end;
		}
		tempBount.close();
		return ret;
	}

	public void read(List<Block> blocks) throws IOException {
		ByteArrayOutputStream bOutput = new ByteArrayOutputStream();
		for (Block block : blocks) {
			bOutput.write(Arrays.copyOfRange(block.getData(), 1, block.data.length));
		}

		// System.out.println("read array:\n" +
		// Arrays.toString(bOutput.toByteArray()));
		DataInputStream bInput = new DataInputStream(new ByteArrayInputStream(bOutput.toByteArray()));
		bOutput.close();
		int num = bInput.readInt();
		int size = 0;
		octants.clear();
		for (int i = 0; i < num; i++) {
			size = bInput.readInt();
			totalSize += size + 4;
			byte[] data = new byte[size];
			bInput.readFully(data);
			octants.add(data);
		}
	}

	public void write(DataOutput output) throws IOException {
		List<Block> blocks = toBlocks();
		for (Block block : blocks) {
			block.write(output);
		}
	}

	public void read(DataInput input) throws IOException {
		List<Block> blocks = new ArrayList<Block>();
		boolean lastBlock = false;
		do {
			Block block = new Block(Block.BLOCKTYPE.DATA_BLOCK, 0);
			block.setBlockIdx(0);
			block.read(input);
			if (block.isDataBlock()) {
				blocks.add(block);
			} else {
				continue;
			}
			lastBlock = block.getData()[0] == 1;
		} while (!lastBlock);
		read(blocks);
	}

	public static class BucketID implements WritableComparable {
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

		public void copy(BucketID other) {
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
			return ((long) blockID) * Block.BLOCK_SIZE;
		}

		@Override
		public boolean equals(Object other) {
			BucketID oBuckID = (BucketID) other;
			return blockID == oBuckID.blockID && offset == oBuckID.offset;
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
		public int compareTo(WritableComparable other) {
			BucketID o = (BucketID) other;
			int ret = Integer.compare(blockID, o.blockID);
			if (ret == 0) {
				ret = Integer.compare(offset, o.offset);
			}
			return ret;
		}
	}

	public BucketID blockIdx() {
		return new BucketID(blockIdx, (short) (octants.size() - 1));
	}

	/**
	 * 当前bucket为空或者空间还足够
	 * 
	 * @param length
	 * @return
	 */
	public boolean canStore(int length) {
		if (octants.isEmpty() || totalSize + length + 4 <= Block.availableSpace()) {
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
		blockIdx = -1;
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
		return "Bucket [blockIdx=" + blockIdx + ", singleBlock=" + singleBlock + ", totalSize=" + totalSize
				+ ", octants=" + octants + "]";
	}

	public void setBlockIdx(int blockIdx) {
		this.blockIdx = blockIdx;
	}

	@Override
	public boolean equals(Object other) {
		Bucket oBuck = (Bucket) other;
		boolean ret = octNum() == oBuck.octNum();
		if (ret) {
			for (int i = 0; i < octNum(); i++) {
				byte[] curData = octants.get(i);
				ret |= curData.equals(oBuck.getOctree(i));
				if (!ret)
					break;
			}
		}
		return ret;
	}

}
