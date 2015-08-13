package core.lsmt;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.util.Comparator;

import core.lsmo.octree.MemoryOctree.OctreeMeta;
import core.lsmt.IMemTable.SSTableMeta;

public interface ISSTableWriter {

	public SSTableMeta getMeta();

	public void write(File dir) throws IOException;

	public void close() throws IOException;

	public static class IntegerComparator implements Comparator<Integer> {
		public static IntegerComparator instance = new IntegerComparator();

		@Override
		public int compare(Integer o1, Integer o2) {
			return Integer.compare(o1, o2);
		}
	}

	public static class DirEntry extends OctreeMeta {
		// runtime state
		public int curKey;
		public int dataStartBlockID;
		public int dataBlockNum;
		public long indexStartOffset;
		public int sampleNum;

		public long getIndexOffset() {
			return indexStartOffset;
		}

		/**
		 * 写出
		 * 
		 * @param output
		 * @throws IOException
		 */
		public void write(DataOutput output) throws IOException {
			output.writeInt(curKey);
			output.writeInt(dataStartBlockID);
			output.writeInt(dataBlockNum);
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
			curKey = input.readInt();
			dataStartBlockID = input.readInt();
			dataBlockNum = input.readInt();
			indexStartOffset = input.readLong();
			sampleNum = input.readInt();
		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see java.lang.Object#toString()
		 */
		@Override
		public String toString() {
			return "DirEntry [curKey=" + curKey + ", dataStartBlockID=" + dataStartBlockID + ", dataBlockNum="
					+ dataBlockNum + ", indexStartOffset=" + indexStartOffset + ", sampleNum=" + sampleNum + "]";
		}
	}

}
