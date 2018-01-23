package core.lsmo.persistence;

import java.io.File;
import java.io.IOException;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

import core.commom.BDBBTreeBuilder;
import core.commom.BDBBtree;
import core.commom.Encoding;
import core.commom.IndexFileUtils;
import core.commom.WritableComparable;
import core.io.BlockOutputStream;
import core.io.Bucket;
import core.lsmo.octree.IOctreeIterator;
import core.lsmo.octree.MemoryOctree;
import core.lsmo.octree.MemoryOctreeIterator;
import core.lsmo.octree.OctreeMerger;
import core.lsmo.octree.OctreeNode;
import core.lsmo.octree.OctreePrepareForWriteVisitor;
import core.lsmt.DirEntry;
import core.lsmt.IMemTable;
import core.lsmt.IMemTable.SSTableMeta;
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
	protected BDBBtree dirMap = null;

	GroupByKeyIterator<WritableComparable, IOctreeIterator> iter;

	protected BlockOutputStream dataBos;
	protected BlockOutputStream markupBos;

	protected DirEntry curDir;

	private int step;

	// state for writing a single octree
	protected int leafOctantNum = 0;
	protected int sentinelOctantNum = 0;

	protected Bucket curDataBuck;
	protected Bucket markupBuck;
	protected SkipCell indexCell;

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

		iter = new GroupByKeyIterator<WritableComparable, IOctreeIterator>(new Comparator<WritableComparable>() {
			@Override
			public int compare(WritableComparable o1, WritableComparable o2) {
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
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public IOctreeSSTableWriter(List<IMemTable> tables, Configuration conf) {
		super(null, conf);

		iter = new GroupByKeyIterator<WritableComparable, IOctreeIterator>(new Comparator<WritableComparable>() {
			@Override
			public int compare(WritableComparable o1, WritableComparable o2) {
				return o1.compareTo(o2);
			}
		});
		int version = 0;
		for (final IMemTable<MemoryOctree> table : tables) {
			version = Math.max(version, table.getMeta().version);
			iter.add(PeekIterDecorate.decorate(new Iterator<Entry<WritableComparable, IOctreeIterator>>() {
				Iterator<Entry<WritableComparable, MemoryOctree>> iter = table.iterator();

				@Override
				public boolean hasNext() {
					return iter.hasNext();
				}

				@Override
				public Entry<WritableComparable, IOctreeIterator> next() {
					Entry<WritableComparable, MemoryOctree> entry = iter.next();
					// an optimization
					entry.getValue().visit(OctreePrepareForWriteVisitor.INSTANCE);
					Entry<WritableComparable, IOctreeIterator> ret = new Pair<WritableComparable, IOctreeIterator>(
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
	 * start writing a new posting list into disk
	 */
	protected abstract DirEntry startNewPostingList(WritableComparable key);

	/**
	 * start writing a new posting list into disk
	 * 
	 * @throws IOException
	 */
	protected abstract void endNewPostingList() throws IOException;

	protected abstract void flushAndNewSentinalBucket() throws IOException;

	protected abstract void flushAndNewSkipCell() throws IOException;

	protected abstract void firstLeafOctantWritten();

	protected abstract void firstSentinelOctantWritten();

	/**
	 * create a new bucket
	 * 
	 * @return
	 * @throws IOException
	 */
	public abstract void flushAndNewDataBucket() throws IOException;

	public abstract void endWritingSSTable() throws IOException;

	protected void addSkipOffset(WritableComparable code) throws IOException {
		if (!indexCell.addIndex(code, curDataBuck.blockIdx())) {
			// flush current index cell, reallocate address for current databuck
			flushAndNewSkipCell();
			indexCell.addIndex(code, curDataBuck.blockIdx());
		}
		if (leafOctantNum == 1 || sentinelOctantNum == 1) {
			curDir.indexStartOffset = indexCell.toFileOffset();
		}
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
			Entry<WritableComparable, List<IOctreeIterator>> entry = iter.next();
			logger.debug(String.format("writing postinglist for key %s", entry.getKey()));
			// start writing a new posting list
			curDir = startNewPostingList(entry.getKey());
			// setup meta values
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
			endNewPostingList();// end a new posting list
		}
		endWritingSSTable();
	}

	@Override
	public void open(File dir) throws IOException {
		if (!dir.exists())
			dir.mkdirs();

		dataBos = new BlockOutputStream(IndexFileUtils.dataFile(dir, meta));
		curDataBuck = new Bucket(dataBos.currentBlockIdx());
		if (conf.standaloneSentinal()) {
			markupBos = new BlockOutputStream(IndexFileUtils.markFile(dir, meta));
			markupBuck = new Bucket(markupBos.currentBlockIdx());
		}

		dirMap = BDBBTreeBuilder.create().setDir(IndexFileUtils.dirMetaFile(dir, meta))
				.setKeyFactory(conf.getDirKeyFactory()).setValueFactory(conf.getDirValueFactory())
				.setAllowDuplicates(false).setReadOnly(false).build();
		dirMap.open();
	}

	protected void writeSentinelOctant(OctreeNode octreeNode) throws IOException {
		byte[] data = octreeNode.toBytes();
		if (conf.standaloneSentinal()) {
			if (!markupBuck.canStore(data.length)) {
				flushAndNewSentinalBucket();
			}
			markupBuck.storeOctant(data);
		} else {
			writeLeafOctant(octreeNode);
		}
	}

	protected void writeLeafOctant(OctreeNode octreeNode) throws IOException {
		byte[] data = octreeNode.toBytes();
		if (!curDataBuck.canStore(data.length)) {
			flushAndNewDataBucket();
		}
		curDataBuck.storeOctant(data);
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
		int totalSegNum = 0;
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
						++leafOctantNum;
					} else {
						++sentinelOctantNum;
					}

					if (octreeNode.size() > 0) {
						totalSegNum += octreeNode.size();
						writeLeafOctant(octreeNode);
						if (leafOctantNum == 1) {
							firstLeafOctantWritten();
						}
					} else if (Encoding.isMarkupNode(octreeNode.getEncoding())) {
						writeSentinelOctant(octreeNode);
						if (sentinelOctantNum == 1) {
							firstSentinelOctantWritten();
						}
					}

					if ((conf.indexLeafOnly() && (leafOctantNum - 1) % step == 0)
							|| (!conf.indexLeafOnly() && (leafOctantNum + sentinelOctantNum - 1) % step == 0))
						addSkipOffset(octreeNode.getEncoding());
				}
			}
		}
		logger.debug(String.format("processed %d segments for term %s, direntry says it should be %d segments",
				totalSegNum, curDir.curKey.toString(), curDir.size));

		// this should never happens!!!
		if (leafOctantNum == 0) {
			firstLeafOctantWritten();
		}
	}

	public void close() throws IOException {
		if (markupBuck.octNum() > 0) {
			markupBos.writeBlocks(markupBuck.toBlocks());
		}

		if (curDataBuck.octNum() > 0) {
			dataBos.writeBlocks(curDataBuck.toBlocks());
		}

		if (dataBos != null) {
			dataBos.close();
			dataBos = null;
		}
		if (markupBos != null) {
			markupBos.close();
			markupBos = null;
		}
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
	 * @return the dataDos
	 */
	public BlockOutputStream getDataDos() {
		return dataBos;
	}

	@Override
	public void moveToDir(File preDir, File dir) {
		File tmpFile = IndexFileUtils.dataFile(preDir, getMeta());
		tmpFile.renameTo(IndexFileUtils.dataFile(dir, getMeta()));

		if (conf.standaloneSentinal()) {
			tmpFile = IndexFileUtils.markFile(preDir, getMeta());
			tmpFile.renameTo(IndexFileUtils.markFile(dir, getMeta()));
		}

		try {
			FileUtils.moveDirectory(IndexFileUtils.dirMetaFile(preDir, meta), IndexFileUtils.dirMetaFile(dir, meta));
		} catch (IOException e) {
			logger.error(e.getMessage());
			throw new RuntimeException(e);
		}
	}

	@Override
	public boolean validate(SSTableMeta meta) {
		File dFile = IndexFileUtils.dataFile(conf.getIndexDir(), meta);
		File markFile = null;
		if (conf.standaloneSentinal()) {
			markFile = IndexFileUtils.markFile(conf.getIndexDir(), meta);
		}
		File dirFile = IndexFileUtils.dirMetaFile(conf.getIndexDir(), meta);
		return dFile.exists() && dirFile.exists() && (markFile == null || markFile.exists());
	}

	@Override
	public void delete(File indexDir, SSTableMeta meta) {
		IndexFileUtils.dataFile(indexDir, meta).delete();
		if (conf.standaloneSentinal()) {
			IndexFileUtils.markFile(indexDir, meta).delete();
		}

		try {
			FileUtils.deleteDirectory(IndexFileUtils.dirMetaFile(indexDir, meta));
		} catch (IOException e) {
		}
	}
}
