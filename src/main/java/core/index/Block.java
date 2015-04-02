package core.index;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

import common.IntegerUtil;

public class Block {
	// typical setting for page size, 4Kb
	public static final int SIZE = 1024 * 4;
	public static final int META_BLOCK = 1;
	public static final int DATA_BLOCK = 2;
	public static final int HEADER_BLOCK = 4;

	// header fields
	int type;
	int recs = 0;
	byte[] bytes;
	int cur = 8;

	public Block(int type) {
		this.type = type;
		bytes = new byte[SIZE];
	}

	public Block(byte[] data) {
		bytes = data;
		// read header
		init();
	}

	/**
	 * 重新加载Block的头
	 */
	public void init() {
		recs = IntegerUtil.readInt(bytes, 0);
		type = recs & 0xff;
		recs = recs >> 8;
		cur = IntegerUtil.readInt(bytes, 4);
	}

	public void reset() {
		// reset header
		// Arrays.fill(bytes, 0, 8, (byte) 0);
		cur = 8;
		recs = 0;
	}

	/**
	 * 以datainputstream的方式读取当前block中的数据
	 * 
	 * @return
	 */
	public DataInputStream readByStream() {
		return new DataInputStream(new ByteArrayInputStream(bytes, 8, cur));
	}

	/**
	 * @return 当前block的记录条数
	 */
	public int getRecs() {
		return recs;
	}

	/**
	 * @return the cur
	 */
	public int getCur() {
		return cur;
	}

	/**
	 * @return the bytes
	 */
	public byte[] getBytes() {
		return bytes;
	}

	/**
	 * 
	 * @param data
	 * @return true如果写入成功
	 */
	public boolean write(byte[] data) {
		boolean ret = true;
		if (data.length + cur <= SIZE) {
			for (int i = 0; i < data.length; i++) {
				bytes[cur++] = data[i];
			}
			recs++;
		} else {
			ret = false;
		}
		return ret;
	}

	/**
	 * 
	 * @param output
	 * @throws IOException
	 */
	public void write(DataOutput output) throws IOException {
		int data = (recs << 8) | type;
		IntegerUtil.writeInt(data, bytes, 0);
		IntegerUtil.writeInt(cur, bytes, 4);
		output.write(bytes);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "Block [recs=" + recs + ", bytes=" + Arrays.toString(bytes)
				+ ", cur=" + cur + "]";
	}

	public int getType() {
		return type;
	}
}
