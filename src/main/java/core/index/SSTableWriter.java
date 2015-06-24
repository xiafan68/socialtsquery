package core.index;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import Util.MergeIterator;
import Util.Pair;
import Util.PeekIterDecorate;
import core.commom.Encoding;
import core.index.MemTable.SSTableMeta;
import core.index.octree.IOctreeIterator;
import core.index.octree.MemoryOctree;
import core.index.octree.MemoryOctree.OctreeMeta;
import core.index.octree.OctreeIterator;
import core.index.octree.OctreeMerger;
import core.index.octree.OctreeZOrderBinaryWriter;
import core.io.Bucket;
import core.io.Bucket.BucketID;

public class SSTableWriter {
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
		public int indexStartID;
		public int sampleNum;

		public long getIndexOffset() {
			return indexStartID;
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
	 * @param tables
	 * @param step
	 */
	public SSTableWriter(List<SSTableReader> tables, int step) {
		
	}
	
	public SSTableWriter(List<MemTable> tables, int step) {
		iter = new MergeIterator<Integer, IOctreeIterator>(
				IntegerComparator.instance);
		int version = 0;
		for (final MemTable table : tables) {
			version = Math.max(version, table.getMeta().version);
			iter.add(PeekIterDecorate
					.decorate(new Iterator<Entry<Integer, IOctreeIterator>>() {
						Iterator<Entry<Integer, MemoryOctree>> iter = table
								.iterator();

						@Override
						public boolean hasNext() {
							return iter.hasNext();
						}

						@Override
						public Entry<Integer, IOctreeIterator> next() {
							Entry<Integer, MemoryOctree> entry = iter.next();
							Entry<Integer, IOctreeIterator> ret = new Pair<Integer, IOctreeIterator>(
									entry.getKey(), new OctreeIterator(entry
											.getValue()));
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

	private SSTableMeta compactMeta(List<SSTableMeta> toCompact) {
		//TODO
		SSTableMeta ret = null;
		return ret;
	}
	
	public SSTableMeta getMeta() {
		return meta;
	}

	public static File dataFile(File dir, SSTableMeta meta) {
		return new File(dir, String.format("%d_%d.data", meta.version,
				meta.level));
	}

	public static File idxFile(File dir, SSTableMeta meta) {
		return new File(dir, String.format("%d_%d.idx", meta.version,
				meta.level));
	}

	public static File dirMetaFile(File dir, SSTableMeta meta) {
		return new File(dir, String.format("%d_%d.meta", meta.version,
				meta.level));
	}

	/**
	 * write the memtable into sstables stored in directory
	 * @param dir
	 * @throws IOException
	 */
	public void write(File dir) throws IOException {
		if (!dir.exists())
			dir.mkdirs();

		dataFileOs = new FileOutputStream(dataFile(dir, meta));
		dataDos = new DataOutputStream(dataFileOs);
		indexFileDos = new FileOutputStream(idxFile(dir, meta));
		indexDos = new DataOutputStream(indexFileDos);
		dirDos = new DataOutputStream(new FileOutputStream(dirMetaFile(dir,
				meta)));

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
			OctreeZOrderBinaryWriter writer = new OctreeZOrderBinaryWriter(
					this, treeIter, step);
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
		code.write(indexDos);
		id.write(indexDos);
	}

	private void startPostingList() throws IOException {
		curDir.dataStartBlockID = (int) (dataFileOs.getChannel().position() / Bucket.BLOCK_SIZE);
		curDir.indexStartID = (int) (indexFileDos.getChannel().position() / Bucket.BLOCK_SIZE);
	}

	private void endPostingList() throws IOException {
		curDir.dataBlockNum = (int) (dataFileOs.getChannel().position() / Bucket.BLOCK_SIZE)
				- curDir.dataStartBlockID;
		dirDos.writeInt(curDir.curKey);
		dirDos.writeInt(curDir.dataStartBlockID);
		dirDos.writeInt(curDir.dataBlockNum);
		dirDos.writeInt(curDir.indexStartID);
		dirDos.writeInt(curDir.sampleNum);
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
