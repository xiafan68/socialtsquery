package core.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import util.Profile;

public class Block {
	public static final int BLOCK_SIZE = 1024 * 4;

	public enum BLOCKTYPE {
		META_BLOCK, DATA_BLOCK, HEADER_BLOCK
	};

	int blockIdx = 0;
	BLOCKTYPE btype;
	byte[] data;

	public Block(BLOCKTYPE btype, int blockIdx) {
		this.btype = btype;
		this.blockIdx = blockIdx;
	}

	public int getBlockIdx() {
		return blockIdx;
	}

	public long getFileOffset() {
		return ((long) blockIdx) * Block.BLOCK_SIZE;
	}

	public void setBlockIdx(int blockIdx) {
		this.blockIdx = blockIdx;
	}

	public byte[] getData() {
		return data;
	}

	public void setData(byte[] data) {
		this.data = data;
	}

	public void write(DataOutput output) throws IOException {
		output.writeInt(btype.ordinal());
		int len = Math.min(data.length, availableSpace());
		output.writeInt(len);
		output.write(data, 0, len);

		// padding bytes
		for (int i = 0; i < availableSpace() - len; i++) {
			output.writeByte(0);
		}
	}

	public void read(DataInput input) throws IOException {
		Profile.instance.updateCounter(Profile.instance.NUM_BLOCK);
		btype = BLOCKTYPE.values()[input.readInt()];
		input.readInt();
		data = new byte[availableSpace()];
		input.readFully(data);
	}

	public boolean isDataBlock() {
		return btype == BLOCKTYPE.DATA_BLOCK;
	}

	public static int availableSpace() {
		return BLOCK_SIZE - 8;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "Bucket [blockIdx=" + blockIdx + "]";
	}

}
