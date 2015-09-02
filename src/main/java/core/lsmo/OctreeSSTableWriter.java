package core.lsmo;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.log4j.Logger;

import Util.Configuration;
import Util.GroupByKeyIterator;
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
import core.lsmt.IMemTable;
import core.lsmt.IMemTable.SSTableMeta;
import core.lsmt.ISSTableReader;
import core.lsmt.ISSTableWriter;
import core.lsmt.WritableComparableKey;

public class OctreeSSTableWriter extends ISSTableWriter {
	private static final Logger logger = Logger.getLogger(OctreeSSTableWriter.class);
	GroupByKeyIterator<WritableComparableKey, IOctreeIterator> iter;
	SSTableMeta meta;

	FileOutputStream dataFileOs;
	DataOutputStream dataDos;
	FileOutputStream indexFileDos;
	DataOutputStream indexDos; // write down the encoding of each block
	DataOutputStream dirDos; // write directory meta
	private int step;
	Configuration conf;
	DirEntry curDir;
	Bucket buck = new Bucket(0);
	private BufferedOutputStream dataBuffer;
	private BufferedOutputStream indexBuffer;
	private BufferedOutputStream dirBuffer;
	private FileOutputStream dirFileOs;

	/**
	 * 用于压缩多个磁盘上的Sstable文件，主要是需要得到一个iter
	 * 
	 * @param tables
	 * @param step
	 */
	public OctreeSSTableWriter(SSTableMeta meta, List<ISSTableReader> tables, Configuration conf) {
		this.meta = meta;
		iter = new GroupByKeyIterator<WritableComparableKey, IOctreeIterator>(new Comparator<WritableComparableKey>() {
			@Override
			public int compare(WritableComparableKey o1, WritableComparableKey o2) {
				return o1.compareTo(o2);
			}
		});
		for (final ISSTableReader table : tables) {
			iter.add(PeekIterDecorate.decorate(new SSTableScanner(table)));
		}
		this.conf = conf;
		this.step = conf.getIndexStep();
		curDir = new DirEntry(conf.getMemTableKey());
	}

	public OctreeSSTableWriter(List<IMemTable> tables, Configuration conf) {
		this.conf = conf;
		iter = new GroupByKeyIterator<WritableComparableKey, IOctreeIterator>(new Comparator<WritableComparableKey>() {
			@Override
			public int compare(WritableComparableKey o1, WritableComparableKey o2) {
				return o1.compareTo(o2);
			}
		});
		int version = 0;
		for (final IMemTable<MemoryOctree> table : tables) {
			version = Math.max(version, table.getMeta().version);
			iter.add(PeekIterDecorate.decorate(new Iterator<Entry<WritableComparableKey, IOctreeIterator>>() {
				Iterator<Entry<WritableComparableKey, MemoryOctree>> iter = table.iterator();

				@Override
				public boolean hasNext() {
					return iter.hasNext();
				}

				@Override
				public Entry<WritableComparableKey, IOctreeIterator> next() {
					Entry<WritableComparableKey, MemoryOctree> entry = iter.next();
					Entry<WritableComparableKey, IOctreeIterator> ret = new Pair<WritableComparableKey, IOctreeIterator>(
							entry.getKey(), new MemoryOctreeIterator(entry.getValue()));
					return ret;
				}

				@Override
				public void remove() {

				}

			}));
		}
		this.meta = new SSTableMeta(version, tables.get(0).getMeta().level);
		this.step = conf.getIndexStep();
		curDir = new DirEntry(conf.getMemTableKey());
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
		openIndexFileWithBuffer(dir);

		while (iter.hasNext()) {
			Entry<WritableComparableKey, List<IOctreeIterator>> entry = iter.next();
			startPostingList();// reset the curDir
			// setup meta values
			curDir.curKey = entry.getKey();
			for (IOctreeIterator iter : entry.getValue()) {
				curDir.merge(iter.getMeta());
			}
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

	private void openIndexFile(File dir) throws FileNotFoundException {
		if (!dir.exists())
			dir.mkdirs();

		dataFileOs = new FileOutputStream(dataFile(dir, meta));
		dataDos = new DataOutputStream(dataFileOs);
		indexFileDos = new FileOutputStream(idxFile(dir, meta));
		indexDos = new DataOutputStream(indexFileDos);
		dirDos = new DataOutputStream(new FileOutputStream(dirMetaFile(dir, meta)));
	}

	private void openIndexFileWithBuffer(File dir) throws FileNotFoundException {
		if (!dir.exists())
			dir.mkdirs();

		dataFileOs = new FileOutputStream(dataFile(dir, meta));
		dataBuffer = new BufferedOutputStream(dataFileOs);
		dataDos = new DataOutputStream(dataBuffer);

		indexFileDos = new FileOutputStream(idxFile(dir, meta));
		indexBuffer = new BufferedOutputStream(indexFileDos);
		indexDos = new DataOutputStream(indexBuffer);

		dirFileOs = new FileOutputStream(dirMetaFile(dir, meta));
		dirBuffer = new BufferedOutputStream(dirFileOs);
		dirDos = new DataOutputStream(dirBuffer);
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
			octreeNode = iter.nextNode();
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
		dataDos.close();
		indexDos.close();
		dirDos.close();

		if (dirBuffer != null)
			dirBuffer.close();
		if (indexBuffer != null)
			indexBuffer.close();
		if (dataBuffer != null)
			dataBuffer.close();

		dataFileOs.close();
		indexFileDos.close();
	}

	public void addSample(Encoding code, BucketID id) throws IOException {
		curDir.sampleNum++;
		code.write(indexDos);
		id.write(indexDos);
	}

	private void startPostingList() throws IOException {
		curDir.startBucketID.copy(buck.blockIdx());
		// curDir.dataStartBlockID = (int) (dataFileOs.getChannel().position() /
		// Bucket.BLOCK_SIZE);
		if (indexBuffer != null)
			indexBuffer.flush();
		curDir.indexStartOffset = indexFileDos.getChannel().position();
		curDir.sampleNum = 0;
		curDir.size = 0;
		curDir.minTime = Integer.MAX_VALUE;
		curDir.maxTime = Integer.MIN_VALUE;
	}

	private void endPostingList() throws IOException {
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
			if (dataBuffer != null)
				dataBuffer.flush();
			buck.setBlockIdx((int) (dataFileOs.getChannel().position() / Bucket.BLOCK_SIZE));
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		return buck;
	}

	@Override
	public void moveToDir(File preDir, File dir) {
		File tmpFile = OctreeSSTableWriter.idxFile(preDir, getMeta());
		tmpFile.renameTo(OctreeSSTableWriter.idxFile(dir, getMeta()));
		tmpFile = OctreeSSTableWriter.dirMetaFile(preDir, getMeta());
		tmpFile.renameTo(OctreeSSTableWriter.dirMetaFile(dir, getMeta()));
		tmpFile = OctreeSSTableWriter.dataFile(preDir, getMeta());
		tmpFile.renameTo(OctreeSSTableWriter.dataFile(dir, getMeta()));
	}
}