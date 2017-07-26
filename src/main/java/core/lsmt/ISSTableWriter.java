package core.lsmt;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.util.Comparator;

import core.io.Bucket;
import core.io.Bucket.BucketID;
import core.lsmo.octree.OctreeNode;
import core.lsmt.IMemTable.SSTableMeta;
import core.lsmt.WritableComparableKey.WritableComparableKeyFactory;
import util.Configuration;

public abstract class ISSTableWriter {
	protected SSTableMeta meta;
	protected Configuration conf;
	protected float splitRatio;
	
	public ISSTableWriter(SSTableMeta meta, Configuration conf){
		this.meta = meta;
		this.conf = conf;
		this.splitRatio = conf.getSplitingRatio();
	}
	
	public abstract SSTableMeta getMeta();

	public abstract void open(File dir) throws IOException;

	/**
	 * 将索引文件写入到dir中
	 * 
	 * @param dir
	 * @throws IOException
	 */
	public abstract void write() throws IOException;

	/**
	 * 验证磁盘上面的数据文件是否全部存在
	 * 
	 * @param meta
	 * @return
	 */
	public abstract boolean validate(SSTableMeta meta);

	/**
	 * 将已写出的索引文件移到dir中
	 * 
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
	 * 
	 * @return
	 */
	public abstract Bucket getDataBucket();

	/**
	 * create a new bucket
	 * 
	 * @return
	 */
	public abstract Bucket newDataBucket();

	public static class DirEntry extends PostingListMeta {
		final WritableComparableKeyFactory factory;
		// runtime state
		public WritableComparableKey curKey = null;
		public BucketID startBucketID = new BucketID();
		public BucketID endBucketID = new BucketID();
		public long indexStartOffset;
		public long sampleNum;

		public DirEntry(WritableComparableKeyFactory factory) {
			this.factory = factory;
		}

		public DirEntry(DirEntry curDir) {
			factory = curDir.factory;
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
			curKey = factory.createIndexKey();
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
			curKey.write(output);
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
			curKey = factory.createIndexKey();
			curKey.read(input);
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
	}

	public abstract void delete(File indexDir, SSTableMeta meta);
	
	public boolean shouldSplitOctant(OctreeNode octreeNode ){
		int[] hist = octreeNode.histogram();
		return octreeNode.getEdgeLen() > 1 && octreeNode.size() > conf.getOctantSizeLimit() * 0.2
		&& (hist[1] == 0 || ((float) hist[0] + 1) / (hist[1] + 1) > splitRatio);
	}
	
}
