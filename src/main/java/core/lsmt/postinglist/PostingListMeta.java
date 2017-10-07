package core.lsmt.postinglist;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import core.commom.Writable;
import io.ByteUtil;

public class PostingListMeta implements Writable {
	public int size = 0;
	public int maxTime = Integer.MIN_VALUE;
	public int minTime = Integer.MAX_VALUE;

	public PostingListMeta() {

	}

	public PostingListMeta(PostingListMeta meta) {
		size = meta.size;
		minTime = meta.minTime;
		maxTime = meta.maxTime;
	}

	public PostingListMeta(PostingListMeta a, PostingListMeta b) {
		size = a.size + b.size;
		minTime = Math.min(a.minTime, b.minTime);
		maxTime = Math.max(a.maxTime, b.maxTime);
	}

	public void merge(PostingListMeta o) {
		size += o.size;
		minTime = Math.min(minTime, o.minTime);
		maxTime = Math.max(maxTime, o.maxTime);
	}

	public void write(DataOutput output) throws IOException {
		ByteUtil.writeVInt(output, size);
		ByteUtil.writeVInt(output, minTime);
		ByteUtil.writeVInt(output, maxTime);
	}

	/**
	 * 读取
	 * 
	 * @param input
	 * @throws IOException
	 */
	public void read(DataInput input) throws IOException {
		size = ByteUtil.readVInt(input);
		minTime = ByteUtil.readVInt(input);
		maxTime = ByteUtil.readVInt(input);
	}

	@Override
	public String toString() {
		return "PostingListMeta [size=" + size + ", maxTime=" + maxTime + ", minTime=" + minTime + "]";
	}
}
