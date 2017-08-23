package core.lsmo.persistence;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

import core.commom.BDBBtree;
import core.commom.Encoding;
import core.io.Block;
import core.io.Bucket;
import core.lsmo.octree.IOctreeIterator;
import core.lsmo.octree.MemoryOctree;
import core.lsmo.octree.MemoryOctreeIterator;
import core.lsmo.octree.OctreeMerger;
import core.lsmo.octree.OctreeNode;
import core.lsmo.octree.OctreePrepareForWriteVisitor;
import core.lsmt.IMemTable;
import core.lsmt.IMemTable.SSTableMeta;
import core.lsmt.WritableComparableKey;
import core.lsmt.postinglist.ISSTableReader;
import core.lsmt.postinglist.ISSTableWriter;
import util.Configuration;
import util.GroupByKeyIterator;
import util.Pair;
import util.PeekIterDecorate;

/**
 * This class defines interfaces needed to write octree into disk in a linear
 * manner.
 * 
 * @author xiafan
 *
 */
public abstract class IOctreeSSTableWriter extends ISSTableWriter {
	private static final Logger logger = Logger.getLogger(IOctreeSSTableWriter.class);

	// stores the directory
	BDBBtree dirMap = null;

	GroupByKeyIterator<WritableComparableKey, IOctreeIterator> iter;

	// for data files
	FileOutputStream dataFileOs;
	DataOutputStream dataDos;
	private BufferedOutputStream dataBuffer;
	/*
	 * the existing of waiting data bucket is caused by the fact that bucket may
	 * not be written in a sequential manner. It happens when we try to write
	 * skip index in data block. First, index blocks are chained by forward
	 * link. Thus, the position of the next index block should be fixed when the
	 * current index block is to be written into disk. Second, an index block
	 * may be overflow after many data blocks have been written (as the index
	 * block contains pointers to those data blocks). However, those data blocks
	 * cann't be actually written to disk if the index block haven not been
	 * written as it has already reserves a position in the disk.
	 */
	List<Bucket> waitingDataBucket = new ArrayList<Bucket>();
	// num of data blocks that are waiting to be flushed
	int unKnowSizeBucketNum = 0;
	int waitingBlockNum = 0;

	private int step;

	// state for writing a single octree
	private int curStep;
	int leafOctantNum = 0;
	int sentinelOctantNum = 0;

	/**
	 * This constructor is used to create writers that merge multiple sstables
	 * into a single one
	 * 
	 * @param meta
	 *            stores metadata of the new sstable
	 * @param tables
	 *            sstables that are to be compacted
	 * @param conf
	 *            configuration
	 */
	public IOctreeSSTableWriter(SSTableMeta meta, List<ISSTableReader> tables, Configuration conf) {
		super(meta, conf);

		iter = new GroupByKeyIterator<WritableComparableKey, IOctreeIterator>(new Comparator<WritableComparableKey>() {
			@Override
			public int compare(WritableComparableKey o1, WritableComparableKey o2) {
				return o1.compareTo(o2);
			}
		});
		for (final ISSTableReader table : tables) {
			iter.add(PeekIterDecorate.decorate(new SSTableScanner(table)));
		}

		this.step = conf.getIndexStep();
	}

	/**
	 * This constructor is used to create writers that flush in-memory octrees
	 * into a disk one
	 * 
	 * @param tables
	 * @param conf
	 */
	public IOctreeSSTableWriter(List<IMemTable> tables, Configuration conf) {
		super(null, conf);

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
					// an optimization
					entry.getValue().visit(OctreePrepareForWriteVisitor.INSTANCE);
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
	}

	/**
	 * write the memtable into sstables stored in directory
	 * 
	 * @param dir
	 *            数据存储目录
	 * @throws IOException
	 */
	public void write() throws IOException {
		while (iter.hasNext()) {
			Entry<WritableComparableKey, List<IOctreeIterator>> entry = iter.next();
			logger.debug(String.format("writing postinglist for key%s", entry.getKey()));
			// start writing a new posting list
			startNewPostingList();
			// setup meta values
			for (IOctreeIterator iter : entry.getValue()) {
				indexHelper.getDirEntry().merge(iter.getMeta());
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
			endNewPostingList();// end a new posting list
		}
	}

	@Override
	public void open(File dir) throws FileNotFoundException {
		if (!dir.exists())
			dir.mkdirs();

		dataFileOs = new FileOutputStream(dataFile(dir, meta));
		dataBuffer = new BufferedOutputStream(dataFileOs);
		dataDos = new DataOutputStream(dataBuffer);

		dirMap = new BDBBtree(dirMetaFile(dir, meta), conf);
		dirMap.open(false, false);
	}

	/**
	 * start writing a new posting list into disk
	 */
	protected abstract void startNewPostingList();

	/**
	 * start writing a new posting list into disk
	 */
	protected abstract void endNewPostingList();

	protected abstract void writeSentinelOctant(OctreeNode octreeNode) throws IOException;

	protected abstract void writeLeafOctant(OctreeNode octreeNode) throws IOException;

	protected abstract void addSkipOffset(WritableComparableKey code) throws IOException;

	protected abstract void firstLeafOctantWritten();

	protected abstract void firstSentinelOctantWritten();

	public Bucket allocateVarLenDataBucket() throws IOException {
		unKnowSizeBucketNum++;
		return new Bucket(currentBlockIdx() + waitingBlockNum, true);
	}

	public Bucket allocateDataBucket() throws IOException {
		return new Bucket(currentBlockIdx() + waitingBlockNum, false);
	}

	public void writeDataBucket(Bucket bucket) {
		if (bucket.isVarLength())
			unKnowSizeBucketNum--;
		if (waitingDataBucket.get(0) == bucket) {
			
		}
	}

	public int currentBlockIdx() throws IOException {
		dataDos.flush();
		dataFileOs.flush();
		return (int) (dataFileOs.getChannel().size() / Block.BLOCK_SIZE);
	}

	/**
	 * write a single octree into disk
	 * 
	 * @param octreeNode
	 * @throws IOException
	 */
	public void writeOctree(IOctreeIterator iter) throws IOException {
		leafOctantNum = 0;
		sentinelOctantNum = 0;
		curStep = 0;
		OctreeNode octreeNode = null;

		while (iter.hasNext()) {
			octreeNode = iter.nextNode();
			if (octreeNode.size() > 0 || Encoding.isMarkupNode(octreeNode.getEncoding())) {
				if (shouldSplitOctant(octreeNode)) {
					octreeNode.split();
					for (int i = 0; i < 8; i++)
						iter.addNode(octreeNode.getChild(i));
				} else {
					if (octreeNode.size() > 0) {
						writeLeafOctant(octreeNode);
						if (++leafOctantNum == 1) {
							firstLeafOctantWritten();
						}
					} else {
						writeSentinelOctant(octreeNode);
						if (++sentinelOctantNum == 1) {
							firstSentinelOctantWritten();
						}
					}

					if (curStep++ % step == 0) {
						addSkipOffset(octreeNode.getEncoding());
					}
				}
			}
		}

		// this should never happens!!!
		if (leafOctantNum == 0) {
			firstLeafOctantWritten();
		}
	}

	public void close() throws IOException {
		dataBuffer.close();
		dataBuffer = null;
		dataDos.close();
		dataFileOs.close();

		dirMap.close();
		dirMap = null;
	}

	@Override
	public void finalize() {
		try {
			close();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
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
	public void moveToDir(File preDir, File dir) {
		File tmpFile = IOctreeSSTableWriter.dataFile(preDir, getMeta());
		tmpFile.renameTo(IOctreeSSTableWriter.dataFile(dir, getMeta()));
		try {
			FileUtils.moveDirectory(dirMetaFile(preDir, meta), dirMetaFile(dir, meta));
		} catch (IOException e) {
			logger.error(e.getMessage());
			throw new RuntimeException(e);
		}
	}

	@Override
	public boolean validate(SSTableMeta meta) {
		File dFile = dataFile(conf.getIndexDir(), meta);
		File dirFile = dirMetaFile(conf.getIndexDir(), meta);
		return dFile.exists() && !dirFile.exists();
	}

	@Override
	public void delete(File indexDir, SSTableMeta meta) {
		dataFile(conf.getIndexDir(), meta).delete();
		try {
			FileUtils.deleteDirectory(dirMetaFile(indexDir, meta));
		} catch (IOException e) {
		}
	}

	public static File dirMetaFile(File dir, SSTableMeta meta) {
		return new File(dir, String.format("%d_%d_dir", meta.version, meta.level));
	}

	public static File dataFile(File dir, SSTableMeta meta) {
		return new File(dir, String.format("%d_%d.data", meta.version, meta.level));
	}
}
