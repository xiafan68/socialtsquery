package core.lsmt;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import core.commom.WritableComparable;
import core.io.Bucket.BucketID;
import core.lsmt.postinglist.PostingListMeta;

public class DirEntry extends PostingListMeta {
	public WritableComparable curKey = null;
	public BucketID startBucketID = new BucketID();
	public BucketID endBucketID = new BucketID();
	public long indexStartOffset;
	public long sampleNum;

	public DirEntry() {

	}

	public DirEntry(DirEntry curDir) {
		curKey = curDir.curKey;
		startBucketID.copy(curDir.startBucketID);
		endBucketID.copy(curDir.endBucketID);
		indexStartOffset = curDir.indexStartOffset;
		sampleNum = curDir.sampleNum;
	}

	public long getIndexOffset() {
		return indexStartOffset;
	}

	public static int indexBlockIdx(long indexOffset) {
		return (int) ((indexOffset >> 32) & 0xffffffff);
	}

	public static int indexOffsetInBlock(long indexOffset) {
		return (int) (indexOffset & 0xffffffff);
	}

	public void init() {
		size = 0;
		minTime = Integer.MAX_VALUE;
		maxTime = Integer.MIN_VALUE;
		sampleNum = 0;
		indexStartOffset = 0;
	}

	/**
	 * 写出
	 * 
	 * @param output
	 * @throws IOException
	 */
	public void write(DataOutput output) throws IOException {
		super.write(output);
		startBucketID.write(output);
		endBucketID.write(output);
		output.writeLong(indexStartOffset);
		output.writeLong(sampleNum);
	}

	/**
	 * 读取
	 * 
	 * @param input
	 * @throws IOException
	 */
	public void read(DataInput input) throws IOException {
		super.read(input);
		startBucketID.read(input);
		endBucketID.read(input);
		indexStartOffset = input.readLong();
		sampleNum = input.readLong();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "DirEntry [curKey=" + curKey + ", startBucketID=" + startBucketID + ", endBucketID=" + endBucketID
				+ ", indexStartOffset=" + indexStartOffset + ", sampleNum=" + sampleNum + "]";
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (!super.equals(obj))
			return false;
		if (getClass() != obj.getClass())
			return false;
		DirEntry other = (DirEntry) obj;
		if (curKey == null) {
			if (other.curKey != null)
				return false;
		} else if (!curKey.equals(other.curKey))
			return false;
		if (endBucketID == null) {
			if (other.endBucketID != null)
				return false;
		} else if (!endBucketID.equals(other.endBucketID))
			return false;
		if (indexStartOffset != other.indexStartOffset)
			return false;
		if (sampleNum != other.sampleNum)
			return false;
		if (startBucketID == null) {
			if (other.startBucketID != null)
				return false;
		} else if (!startBucketID.equals(other.startBucketID))
			return false;
		return true;
	}

}