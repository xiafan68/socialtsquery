package core.index;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import org.apache.log4j.Logger;

import Util.MergeIterator;
import Util.Pair;
import Util.PeekIterDecorate;
import core.commom.Encoding;
import core.index.MemTable.SSTableMeta;
import core.index.octree.IOctreeIterator;
import core.index.octree.MemoryOctree;
import core.index.octree.MemoryOctree.OctreeMeta;
import core.index.octree.MemoryOctreeIterator;
import core.index.octree.OctreeMerger;
import core.index.octree.OctreeZOrderBinaryWriter;
import core.io.Bucket;
import core.io.Bucket.BucketID;

public class SSTableWriter {
	private static final Logger logger = Logger.getLogger(SSTableWriter.class);
	MergeIterator<Integer, IOctreeIterator> iter;
	SSTableMeta meta;

	FileOutputStream dataFileOs;
	DataOutputStream dataDos;
	FileOutputStream indexFileDos;
	DataOutputStream indexDos; // write down the encoding of each block
	DataOutputStream dirDos; // write directory meta
	private int step;

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

	DirEntry curDir = new DirEntry();

	public static class IntegerComparator implements Comparator<Integer> {
		public static IntegerComparator instance = new IntegerComparator();

		@Override
		public int compare(Integer o1, Integer o2) {
			return Integer.compare(o1, o2);
		}
	}

	/**
	 * 用于压缩多个磁盘上的Sstable文件，主要是需要得到一个iter
	 * 
	 * @param tables
	 * @param step
	 */
	public SSTableWriter(SSTableMeta meta, List<ISSTableReader> tables, int step) {
		this.meta = meta;
		iter = new MergeIterator<Integer, IOctreeIterator>(IntegerComparator.instance);
		for (final ISSTableReader table : tables) {
			iter.add(PeekIterDecorate.decorate(new SSTableScanner(table)));
		}
		this.step = step;
	}

	public SSTableWriter(List<MemTable> tables, int step) {
		iter = new MergeIterator<Integer, IOctreeIterator>(IntegerComparator.instance);
		int version = 0;
		for (final MemTable table : tables) {
			version = Math.max(version, table.getMeta().version);
			iter.add(PeekIterDecorate.decorate(new Iterator<Entry<Integer, IOctreeIterator>>() {
				Iterator<Entry<Integer, MemoryOctree>> iter = table.iterator();

				@Override
				public boolean hasNext() {
					return iter.hasNext();
				}

				@Override
				public Entry<Integer, IOctreeIterator> next() {
					Entry<Integer, MemoryOctree> entry = iter.next();
					Entry<Integer, IOctreeIterator> ret = new Pair<Integer, IOctreeIterator>(entry.getKey(),
							new MemoryOctreeIterator(entry.getValue()));
					return ret;
				}

				@Override
				public void remove() {

				}

			}));
		}
		this.meta = new SSTableMeta(version, tables.get(0).getMeta().level);
		this.step = step;
	}

	public SSTableMeta getMeta() {
		return meta;
	}

	public static File dataFile(File dir, SSTableMeta meta) {
		return new File(dir, String.format("%d_%d.data", meta.version, meta.level));
	}

	public static File idxFile(File dir, SSTableMeta meta) {
		return new File(dir, String.format("%d_%d.idx", meta.version, meta.level));
	}

	public static File dirMetaFile(File dir, SSTableMeta meta) {
		return new File(dir, String.format("%d_%d.meta", meta.version, meta.level));
	}

	/**
	 * write the memtable into sstables stored in directory
	 * 
	 * @param dir
	 *            数据存储目录
	 * @throws IOException
	 */
	public void write(File dir) throws IOException {
		if (!dir.exists())
			dir.mkdirs();

		dataFileOs = new FileOutputStream(dataFile(dir, meta));
		dataDos = new DataOutputStream(dataFileOs);
		indexFileDos = new FileOutputStream(idxFile(dir, meta));
		indexDos = new DataOutputStream(indexFileDos);
		dirDos = new DataOutputStream(new FileOutputStream(dirMetaFile(dir, meta)));

		while (iter.hasNext()) {
			Entry<Integer, List<IOctreeIterator>> entry = iter.next();
			curDir.curKey = entry.getKey();
			startPostingList();// write a new posting
			List<IOctreeIterator> trees = entry.getValue();
			IOctreeIterator treeIter = null;

			treeIter = trees.get(0);
			if (trees.size() > 1) {
				for (IOctreeIterator curIter : trees.subList(1, trees.size())) {
					treeIter = new OctreeMerger(treeIter, curIter);
				}
			}
			OctreeZOrderBinaryWriter writer = new OctreeZOrderBinaryWriter(this, treeIter, step);
			writer.write();
			writer.close();

			endPostingList();// end a new posting list
		}
	}

	public void close() throws IOException {
		dataFileOs.close();
		dataDos.close();
		indexFileDos.close();
		indexDos.close();
		dirDos.close();
	}

	public void addSample(Encoding code, BucketID id) throws IOException {
		curDir.sampleNum++;
		code.write(indexDos);
		id.write(indexDos);
	}

	private void startPostingList() throws IOException {
		curDir.dataStartBlockID = (int) (dataFileOs.getChannel().position() / Bucket.BLOCK_SIZE);
		curDir.indexStartOffset = indexFileDos.getChannel().position();
		curDir.sampleNum = 0;
	}

	private void endPostingList() throws IOException {
		curDir.dataBlockNum = (int) (dataFileOs.getChannel().position() / Bucket.BLOCK_SIZE) - curDir.dataStartBlockID;
		curDir.write(dirDos);
		logger.debug("finish " + curDir);
	}

	/**
	 * @return the dataFileOs
	 * @throws IOException
	 */
	public long getDataFilePosition() throws IOException {
		return dataFileOs.getChannel().position();
	}

	/**
	 * @return the dataDos
	 */
	public DataOutputStream getDataDos() {
		return dataDos;
	}

}
