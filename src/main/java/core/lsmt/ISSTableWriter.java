package core.lsmt;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.util.Comparator;

import core.io.Bucket;
import core.io.Bucket.BucketID;
import core.lsmt.IMemTable.SSTableMeta;

public abstract class ISSTableWriter {

	public abstract SSTableMeta getMeta();

	/**
	 * 将索引文件写入到dir中
	 * @param dir
	 * @throws IOException
	 */
	public abstract void write(File dir) throws IOException;

	/**
	 * 将已写出的索引文件移到dir中
	 * @param dir
	 */
	public abstract void moveToDir(File preDir, File dir);

	public abstract void close() throws IOException;

	public static class IntegerComparator implements Comparator<Integer> {
		public static IntegerComparator instance = new IntegerComparator();

		@Override
		public int compare(Integer o1, Integer o2) {
			return Integer.compare(o1, o2);
		}
	}

	/**
	 * return a bucket
	 * @return
	 */
	public abstract Bucket getBucket();

	/**
	 * create a new bucket
	 * @return
	 */
	public abstract Bucket newBucket();

	public static class DirEntry extends PostingListMeta {
		// runtime state
		public int curKey;
		public BucketID startBucketID;
		public BucketID endBucketID;
		public long indexStartOffset;
		public int sampleNum;

		public long getIndexOffset() {
			return indexStartOffset;
		}

		public void init() {
			curKey = 0;
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
			output.writeInt(curKey);
			startBucketID.write(output);
			endBucketID.write(output);
			output.writeLong(indexStartOffset);
			output.writeInt(sampleNum);
		}

		/**
		 * 读取
		 * 
		 * @param input
		 * @throws IOException
		 */
		public void read(DataInput input) throws IOException {
			super.read(input);
			curKey = input.readInt();
			startBucketID.read(input);
			endBucketID.read(input);
			indexStartOffset = input.readLong();
			sampleNum = input.readInt();
		}

		/* (non-Javadoc)
		 * @see java.lang.Object#toString()
		 */
		@Override
		public String toString() {
			return "DirEntry [curKey=" + curKey + ", startBucketID="
					+ startBucketID + ", endBucketID=" + endBucketID
					+ ", indexStartOffset=" + indexStartOffset + ", sampleNum="
					+ sampleNum + "]";
		}
	}
}
