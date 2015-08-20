package core.lsmo;

import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import org.apache.log4j.Logger;

import Util.MergeIterator;
import Util.Pair;
import Util.PeekIterDecorate;
import core.commom.Encoding;
import core.io.Bucket;
import core.io.Bucket.BucketID;
import core.lsmo.octree.IOctreeIterator;
import core.lsmo.octree.MemoryOctree;
import core.lsmo.octree.MemoryOctreeIterator;
import core.lsmo.octree.OctreeMerger;
import core.lsmo.octree.OctreeNode;
import core.lsmo.octree.OctreeZOrderBinaryWriter;
import core.lsmo.octree.MemoryOctree.OctreeMeta;
import core.lsmt.IMemTable;
import core.lsmt.IMemTable.SSTableMeta;
import core.lsmt.ISSTableReader;
import core.lsmt.ISSTableWriter;

public class SSTableWriter implements ISSTableWriter {
	private static final Logger logger = Logger.getLogger(SSTableWriter.class);
	MergeIterator<Integer, IOctreeIterator> iter;
	SSTableMeta meta;

	FileOutputStream dataFileOs;
	DataOutputStream dataDos;
	FileOutputStream indexFileDos;
	DataOutputStream indexDos; // write down the encoding of each block
	DataOutputStream dirDos; // write directory meta
	private int step;

	DirEntry curDir = new DirEntry();
	Bucket buck = new Bucket(0);

	public static class DirEntry extends OctreeMeta {
		// runtime state
		public int curKey;
		public BucketID startBucketID;
		public BucketID endBucketID;
		public long indexStartOffset;
		public int sampleNum;

		public DirEntry() {
			startBucketID = new BucketID();
			endBucketID = new BucketID();
		}

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
			curKey = input.readInt();
			startBucketID.read(input);
			endBucketID.read(input);
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
			return "DirEntry [curKey=" + curKey + ", startBucketID=" + startBucketID + ", endBucketID=" + endBucketID
					+ ", indexStartOffset=" + indexStartOffset + ", sampleNum=" + sampleNum + "]";
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

	public SSTableWriter(List<IMemTable> tables, int step) {
		iter = new MergeIterator<Integer, IOctreeIterator>(IntegerComparator.instance);
		int version = 0;
		for (final IMemTable<MemoryOctree> table : tables) {
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
			writeOctree(treeIter);
			endPostingList();// end a new posting list
		}
	}

	/**
	 * the visiting of the parent controls the setup of bucket
	 * 
	 * @param octreeNode
	 * @throws IOException
	 */
	public void writeOctree(IOctreeIterator iter) throws IOException {
		boolean first = true;
		OctreeNode octreeNode = null;
		int count = 0;
		while (iter.hasNext()) {
			octreeNode = iter.next();
			if (octreeNode.size() > 0 || OctreeNode.isMarkupNode(octreeNode.getEncoding())) {
				int[] counters = octreeNode.histogram();
				if (octreeNode.getEdgeLen() != 1 && counters[0] > (MemoryOctree.size_threshold >> 1)) {
					octreeNode.split();
					for (int i = 0; i < 8; i++)
						iter.addNode(octreeNode.getChild(i));
				} else {
					ByteArrayOutputStream baos = new ByteArrayOutputStream();
					DataOutputStream dos = new DataOutputStream(baos);
					// first write the octant code, then write the octant
					octreeNode.getEncoding().write(dos);
					octreeNode.write(dos);
					byte[] data = baos.toByteArray();
					if (!buck.canStore(data.length)) {
						buck.write(getDataDos());
						logger.debug(buck);
						newBucket();
					}
					logger.debug(octreeNode);
					buck.storeOctant(data);
					if (first) {
						first = false;
						startPostingList();
					}
					if (count++ % step == 0) {
						addSample(octreeNode.getEncoding(), buck.blockIdx());
					}
				}
			}
		}
	}

	public void close() throws IOException {
		if (buck != null) {
			buck.write(getDataDos());
			logger.debug("last bucket:" + buck.toString());
		}

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

	public void startPostingList() throws IOException {
		curDir.startBucketID.copy(buck.blockIdx());
		// curDir.dataStartBlockID = (int) (dataFileOs.getChannel().position() /
		// Bucket.BLOCK_SIZE);
		curDir.indexStartOffset = indexFileDos.getChannel().position();
		curDir.sampleNum = 0;
	}

	public void endPostingList() throws IOException {
		// curDir.dataBlockNum = (int) (dataFileOs.getChannel().position() /
		// Bucket.BLOCK_SIZE)
		// - curDir.dataStartBlockID;
		curDir.endBucketID.copy(buck.blockIdx());
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

	@Override
	public Bucket getBucket() {
		return buck;
	}

	@Override
	public Bucket newBucket() {
		buck.reset();
		try {
			buck.setBlockIdx((int) (dataFileOs.getChannel().position() / Bucket.BLOCK_SIZE));
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		return buck;
	}
}
