package core.lsmo.internformat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.log4j.Logger;

import core.commom.BDBBtree;
import core.io.Block;
import core.io.Bucket;
import core.io.Bucket.BucketID;
import core.lsmo.bdbformat.OctreeSSTableWriter;
import core.lsmo.bdbformat.SSTableScanner;
import core.lsmo.octree.IOctreeIterator;
import core.lsmo.octree.MemoryOctree;
import core.lsmo.octree.MemoryOctreeIterator;
import core.lsmo.octree.OctreeMerger;
import core.lsmo.octree.OctreeNode;
import core.lsmo.octree.OctreePrepareForWriteVisitor;
import core.lsmt.IMemTable;
import core.lsmt.IMemTable.SSTableMeta;
import core.lsmt.WritableComparableKey.WritableComparableKeyFactory;
import util.Configuration;
import util.GroupByKeyIterator;
import util.Pair;
import util.PeekIterDecorate;
import core.lsmt.ISSTableReader;
import core.lsmt.ISSTableWriter;
import core.lsmt.IndexHelper;
import core.lsmt.WritableComparableKey;

/**
 * 把meta data写入到data文件中
 * 
 * ----------------------------------------------------------------------------
 * metablock blockheader encoding1->boffset+loc3 encoding2->boffset2+loc3
 * encoding3+boffset3+loc3
 * ----------------------------------------------------------------------------
 * datablock blockheader numofoctant octant1 octant2 octant3
 * ----------------------------------------------------------------------------
 * datablock blockheader numofoctant octant1 octant2 octant3
 * ----------------------------------------------------------------------------
 * 
 * 
 * DirEntry应该记录 startoffset, endoffset indexoffset, samplenum
 * 
 * @author xiafan
 */
public class InternOctreeSSTableWriter extends ISSTableWriter {
	private static final Logger logger = Logger.getLogger(InternOctreeSSTableWriter.class);
	GroupByKeyIterator<WritableComparableKey, IOctreeIterator> iter;

	FileOutputStream dataFileOs;
	DataOutputStream dataDos;
	// private BufferedOutputStream dataBuffer;

	FileOutputStream markFileOs;
	DataOutputStream markDos;

	private int step;
	Configuration conf;
	InternIndexHelper indexHelper;
	float splitingRatio = 2;

	/**
	 * 用于压缩多个磁盘上的Sstable文件，主要是需要得到一个iter
	 * 
	 * @param meta新生成文件的元数据
	 * @param tables
	 *            需要压缩的表
	 * @param conf
	 *            配置信息
	 */
	public InternOctreeSSTableWriter(SSTableMeta meta, List<ISSTableReader> tables, Configuration conf) {
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
		indexHelper = new InternIndexHelper(conf);
		splitingRatio = conf.getSplitingRatio();
	}

	public InternOctreeSSTableWriter(List<IMemTable> tables, Configuration conf) {
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
		indexHelper = new InternIndexHelper(conf);
		splitingRatio = conf.getSplitingRatio();
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
			// indexHelper.startPostingList(entry.getKey(), null);
			// setup meta values
			indexHelper.startPostingList(entry.getKey(), null);
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
			indexHelper.endPostingList(null);// end a new posting list
		}
	}

	/**
	 * the visiting of the parent controls the setup of bucket
	 * 
	 * @param octreeNode
	 * @throws IOException
	 */
	public void writeOctree(IOctreeIterator iter) throws IOException {
		int size = 0;
		OctreeNode octreeNode = null;
		while (iter.hasNext()) {
			octreeNode = iter.nextNode();
			if (octreeNode.size() > 0 || OctreeNode.isMarkupNode(octreeNode.getEncoding())) {
				if (octreeNode.size() > 0) {
					int[] hist = octreeNode.histogram();
					// octreeNode.size() > MemoryOctree.size_threshold * 0.5
					if (octreeNode.getEdgeLen() > 1 && octreeNode.size() > conf.getOctantSizeLimit() * 0.2
							&& (hist[1] == 0 || ((float) hist[0] + 1) / (hist[1] + 1) > splitingRatio)) {
						// 下半部分是上半部分的两倍
						octreeNode.split();
						for (int i = 0; i < 8; i++)
							iter.addNode(octreeNode.getChild(i));
					} else {
						size += octreeNode.size();
						indexHelper.addOctant(octreeNode);
					}
				} else {
					size += octreeNode.size();
					indexHelper.addOctant(octreeNode);
				}
			}
		}
		if (size != iter.getMeta().size) {
			System.err.println(size + " size do not  equals + " + iter.getMeta().size);
		}
	}

	public void close() throws IOException {
		if (dataDos != null) {
			indexHelper.close();

			dataDos.close();
			dataDos = null;
			dataFileOs.close();

			markDos.close();
			markFileOs.close();
		}
	}

	@Override
	public void finalize() {
		try {
			close();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public Bucket getDataBucket() {
		return null;
	}

	@Override
	public Bucket newDataBucket() {
		return null;
	}

	@Override
	public void moveToDir(File preDir, File dir) {
		File tmpFile = dataFile(preDir, getMeta());
		tmpFile.renameTo(dataFile(dir, getMeta()));
		tmpFile = markFile(preDir, getMeta());
		tmpFile.renameTo(markFile(dir, getMeta()));
		indexHelper.moveToDir(preDir, dir, meta);
	}

	@Override
	public boolean validate(SSTableMeta meta) {
		File dFile = dataFile(conf.getIndexDir(), meta);
		if (!dFile.exists())
			return false;
		return indexHelper.validate(meta);
	}

	@Override
	public void delete(File indexDir, SSTableMeta meta) {
		dataFile(conf.getIndexDir(), meta).delete();
		markFile(conf.getIndexDir(), meta).delete();
		indexHelper.delete(conf.getIndexDir(), meta);
	}

	public static File dirMetaFile(File dir, SSTableMeta meta) {
		return new File(dir, String.format("%d_%d_dir", meta.version, meta.level));
	}

	public int currentBlockIdx() throws IOException {
		dataDos.flush();
		dataFileOs.flush();
		return (int) (dataFileOs.getChannel().size() / Block.BLOCK_SIZE);
	}

	/**
	 * 流程： 1. 写dataBuck 2. 抽样，如果命中，写入buck 3. 如果buck写满？
	 * 
	 * metablock dblock dblock dblock dblock; metablock
	 * 
	 * @author xiafan
	 *
	 */
	private class InternIndexHelper extends IndexHelper {

		BDBBtree dirMap = null;

		SkipCell cell;
		// 用于记录cell之后的data blocks
		ByteArrayOutputStream tempDataBout = new ByteArrayOutputStream();
		DataOutputStream tempDataDos = new DataOutputStream(tempDataBout);

		Bucket markUpBuck;
		Bucket dataBuck;
		List<DirEntry> dirsStartInCurBuck = new ArrayList<DirEntry>();// 起始于最后一个buck的dirs
		List<DirEntry> dirsEndInCurBuck = new ArrayList<DirEntry>();// 起始于最后一个buck的dirs
		int curStep = 0;
		boolean writeFirstBlock = true;
		boolean sampleFirstIndex = true;

		private boolean writeFirstMark = true;

		public InternIndexHelper(Configuration conf) {
			super(conf);
		}

		@Override
		public void openIndexFile(File dir, SSTableMeta meta) throws IOException {
			dirMap = new BDBBtree(dirMetaFile(dir, meta), conf);
			dirMap.open(false, false);

			cell = new SkipCell(currentBlockIdx(), conf.getIndexValueFactory());
			dataBuck = new Bucket((cell.getBlockIdx() + 1) * Block.BLOCK_SIZE);
			markUpBuck = new Bucket(currentMarkIdx() * Block.BLOCK_SIZE);
		}

		private int currentMarkIdx() throws IOException {
			return (int) (markFileOs.getChannel().size() / Block.BLOCK_SIZE);
		}

		@Override
		public void startPostingList(WritableComparableKey key, BucketID newListStart) throws IOException {
			curDir = new MarkDirEntry(conf.getIndexKeyFactory());
			curDir.curKey = key;
			writeFirstBlock = true;
			sampleFirstIndex = true;
			writeFirstMark = true;

		}

		/**
		 * 当当前posting list第一次存入了dataBuck时才调用这个函数
		 * 
		 * @throws IOException
		 */
		public void startPostingList() throws IOException {
			curDir.startBucketID.copy(dataBuck.blockIdx());
			curStep = 0;
			dirsStartInCurBuck.add(curDir);
			writeFirstBlock = false;
		}

		@Override
		public void endPostingList(BucketID postingListEnd) throws IOException {
			curDir.endBucketID.copy(dataBuck.blockIdx());
			dirsEndInCurBuck.add(curDir);
			curDir.sampleNum = cell.toFileOffset();
		}

		private int getCurBuckBlockID() {
			return cell.getBlockIdx() + tempDataDos.size() / Block.BLOCK_SIZE + 1;
		}

		@Override
		public void buildIndex(WritableComparableKey code, BucketID id) throws IOException {
			if (!cell.addIndex(code, dataBuck.blockIdx())) {
				// 创建新的skip cell
				// first write the meta data
				cell.write(getCurBuckBlockID()).write(dataDos);

				// then write data blocks
				tempDataBout.writeTo(dataDos);
				tempDataBout.reset();
				tempDataDos = new DataOutputStream(tempDataBout);

				cell.reset();
				cell.setBlockIdx(currentBlockIdx());
				// setup new context
				dataBuck.setBlockIdx(getCurBuckBlockID());
				for (DirEntry entry : dirsStartInCurBuck) {
					entry.startBucketID.blockID = dataBuck.blockIdx().blockID;
				}

				for (DirEntry entry : dirsEndInCurBuck) {
					entry.endBucketID.blockID = dataBuck.blockIdx().blockID;
				}
				cell.addIndex(code, dataBuck.blockIdx());
			}
			if (sampleFirstIndex) {
				curDir.indexStartOffset = cell.toFileOffset();
				sampleFirstIndex = false;
			}
		}

		public void flushSkipCell() throws IOException {
			// 创建新的skip cell
			// first write the meta data
			cell.write(-1).write(dataDos);
			cell.reset();
			// then write data blocks
			tempDataBout.writeTo(dataDos);
			tempDataBout.close();
			tempDataBout = null;
		}

		public void addOctant(OctreeNode node) throws IOException {
			// store the current node
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			DataOutputStream dos = new DataOutputStream(baos);
			// first write the octant code, then write the octant
			node.getEncoding().write(dos);
			node.write(dos);
			byte[] data = baos.toByteArray();
			// && OctreeNode.isMarkupNode(node.getEncoding())
			if (node.size() == 0) {
				if (!markUpBuck.canStore(data.length)) {
					markUpBuck.write(markDos);
					markDos.flush();
					markUpBuck.reset();
					markUpBuck.setBlockIdx(currentMarkIdx());
				}
				markUpBuck.storeOctant(data);
				if (writeFirstMark) {
					((MarkDirEntry) curDir).startMarkOffset.copy(markUpBuck.blockIdx());
					writeFirstMark = false;
				}
				((MarkDirEntry) curDir).markNum++;
			} else {
				if (!dataBuck.canStore(data.length)) {
					flushLastBuck();
				}
				dataBuck.storeOctant(data);

				if (writeFirstBlock) {
					this.startPostingList();
				}

				// sample index
				if (curStep++ % step == 0) {
					buildIndex(node.getEncoding(), null);
				}
			}
		}

		public void flushLastBuck() throws IOException {
			dataBuck.write(tempDataDos);
			dataBuck.reset();
			dataBuck.setBlockIdx(getCurBuckBlockID());

			for (DirEntry entry : dirsEndInCurBuck) {
				// if (entry.curKey.toString().equals("0")) {
				// System.out.println();
				// }
				dirMap.insert(entry.curKey, entry);
			}
			cell.newBucket();
			// 以下两个字段均已确定
			dirsStartInCurBuck.clear();
			dirsEndInCurBuck.clear();
		}

		@Override
		public void moveToDir(File preDir, File dir, SSTableMeta meta) {
			try {
				FileUtils.moveDirectory(dirMetaFile(preDir, meta), dirMetaFile(dir, meta));
			} catch (IOException e) {
				logger.error(e.getMessage());
			}
		}

		@Override
		public void close() throws IOException {
			if (tempDataBout != null) {
				flushLastBuck();
				flushSkipCell();
				markUpBuck.write(markDos);
				dirMap.close();
			}
		}

		@Override
		public boolean validate(SSTableMeta meta) {
			return true;
		}

		@Override
		public boolean delete(File indexDir, SSTableMeta meta) {
			try {
				FileUtils.deleteDirectory(dirMetaFile(indexDir, meta));
				return true;
			} catch (IOException e) {
			}
			return false;
		}
	}

	public static File dataFile(File dir, SSTableMeta meta) {
		return new File(dir, String.format("%d_%d.data", meta.version, meta.level));
	}

	public static File markFile(File dir, SSTableMeta meta) {
		return new File(dir, String.format("%d_%d.mark", meta.version, meta.level));
	}

	@Override
	public SSTableMeta getMeta() {
		return meta;
	}

	@Override
	public void open(File dir) throws IOException {
		if (!dir.exists())
			dir.mkdirs();

		dataFileOs = new FileOutputStream(dataFile(dir, meta));
		// dataBuffer = new BufferedOutputStream(dataFileOs);
		dataDos = new DataOutputStream(dataFileOs);

		markFileOs = new FileOutputStream(markFile(dir, meta));
		markDos = new DataOutputStream(markFileOs);
		indexHelper.openIndexFile(dir, meta);
	}

	public static class MarkDirEntry extends DirEntry {
		public BucketID startMarkOffset = new BucketID(0, (short) 0);
		public int markNum = 0;

		public MarkDirEntry(WritableComparableKeyFactory factory) {
			super(factory);
		}

		public MarkDirEntry(DirEntry curDir) {
			super(curDir);
		}

		@Override
		public void write(DataOutput output) throws IOException {
			super.write(output);
			startMarkOffset.write(output);
			output.writeInt(markNum);
		}

		@Override
		public void read(DataInput input) throws IOException {
			super.read(input);
			startMarkOffset.read(input);
			markNum = input.readInt();
		}

	}
}
