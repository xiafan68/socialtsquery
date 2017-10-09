package core.lsmt.postinglist;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.util.Comparator;

import core.commom.WritableComparable;
import core.commom.WritableComparable.WritableComparableFactory;
import core.io.Bucket;
import core.io.Bucket.BucketID;
import core.lsmo.octree.OctreeNode;
import core.lsmt.IMemTable.SSTableMeta;
import util.Configuration;

/**
 * This class defines the interfaces needed to write a posting list into disk
 * @author xiafan
 * @date 2017/08/23
 *
 */
public abstract class ISSTableWriter {
	protected SSTableMeta meta;
	protected Configuration conf;
	protected float splitRatio;
	
	public ISSTableWriter(SSTableMeta meta, Configuration conf){
		this.meta = meta;
		this.conf = conf;
		this.splitRatio = conf.getSplitingRatio();
	}
	
	public SSTableMeta getMeta(){
		return meta;
	}

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

	

	public abstract void delete(File indexDir, SSTableMeta meta) throws IOException;
	
	public boolean shouldSplitOctant(OctreeNode octreeNode ){
		int[] hist = octreeNode.histogram();
		return octreeNode.getEdgeLen() > 1 && octreeNode.size() > conf.getOctantSizeLimit() * 0.2
		&& (hist[1] == 0 || ((float) hist[0] + 1) / (hist[1] + 1) > splitRatio);
	}
	
}
