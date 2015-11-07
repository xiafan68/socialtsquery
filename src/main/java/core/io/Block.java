package core.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Block {
	public static final int BLOCK_SIZE = 1024 * 4;

	public enum BLOCKTYPE {
		META_BLOCK, DATA_BLOCK, HEADER_BLOCK
	};

	int blockIdx = 0;
	BLOCKTYPE btype;
	byte[] data;

	public Block(BLOCKTYPE btype, long offset) {
		this.btype = btype;
		this.blockIdx = (int) (offset / BLOCK_SIZE);
	}

	public void setData(byte[] data) {
		if (data.length > availableSpace()) {
			throw new IllegalArgumentException("the size of data is larger than the data space");
		}
		this.data = data;
	}

	public void write(DataOutput output) throws IOException {
		output.writeInt(btype.ordinal());
		output.writeInt(data.length);
		output.write(data);

		// padding bytes
		for (int i = 0; i < BLOCK_SIZE - 8 - data.length; i++) {
			output.writeByte(0);
		}
	}

	public void read(DataInput input) throws IOException {

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
