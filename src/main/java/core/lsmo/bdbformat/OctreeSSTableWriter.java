package core.lsmo.bdbformat;

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
import util.Configuration;
import util.GroupByKeyIterator;
import util.Pair;
import util.PeekIterDecorate;
import core.lsmt.ISSTableReader;
import core.lsmt.ISSTableWriter;
import core.lsmt.IndexHelper;
import core.lsmt.WritableComparableKey;

public class OctreeSSTableWriter extends ISSTableWriter {
	private static final Logger logger = Logger.getLogger(OctreeSSTableWriter.class);
	GroupByKeyIterator<WritableComparableKey, IOctreeIterator> iter;

	FileOutputStream dataFileOs;
	DataOutputStream dataDos;
	private BufferedOutputStream dataBuffer;

	private int step;
	Bucket buck = new Bucket(0);
	IndexHelper indexHelper;

	/**
	 * 用于压缩多个磁盘上的Sstable文件，主要是需要得到一个iter
	 * 
	 * @param meta新生成文件的元数据
	 * @param tables
	 *            需要压缩的表
	 * @param conf
	 *            配置信息
	 */
	public OctreeSSTableWriter(SSTableMeta meta, List<ISSTableReader> tables, Configuration conf) {
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
		try {
			indexHelper = (IndexHelper) Class.forName(conf.getIndexHelper()).getConstructor(Configuration.class)
					.newInstance(conf);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public OctreeSSTableWriter(List<IMemTable> tables, Configuration conf) {
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
		try {
			indexHelper = (IndexHelper) Class.forName(conf.getIndexHelper()).getConstructor(Configuration.class)
					.newInstance(conf);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public SSTableMeta getMeta() {
		return meta;
	}

	public static File dataFile(File dir, SSTableMeta meta) {
		return new File(dir, String.format("%d_%d.data", meta.version, meta.level));
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
			indexHelper.startPostingList(entry.getKey(), buck.blockIdx());
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
			endPostingList();// end a new posting list
		}
	}

	@Override
	public void open(File dir) throws FileNotFoundException {
		if (!dir.exists())
			dir.mkdirs();

		dataFileOs = new FileOutputStream(dataFile(dir, meta));
		dataBuffer = new BufferedOutputStream(dataFileOs);
		dataDos = new DataOutputStream(dataBuffer);

		try {
			indexHelper.openIndexFile(dir, meta);
		} catch (Exception e) {
			throw new RuntimeException(e);
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
			octreeNode = iter.nextNode();
			if (octreeNode.size() > 0 || OctreeNode.isMarkupNode(octreeNode.getEncoding())) {
				if (shouldSplitOctant(octreeNode)) {
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
						newDataBucket();
					}
					logger.debug(octreeNode);
					buck.storeOctant(data);
					if (first) {
						first = false;
						indexHelper.setupDataStartBlockIdx(buck.blockIdx());
					}
					if (count++ % step == 0) {
						indexHelper.buildIndex(octreeNode.getEncoding(), buck.blockIdx());
					}
				}
			}
		}
		if (first) {
			indexHelper.setupDataStartBlockIdx(buck.blockIdx());
		}
	}

	public void close() throws IOException {
		if (dataBuffer != null) {
			if (buck != null) {
				buck.write(getDataDos());
				logger.debug("last bucket:" + buck.toString());
			}

			dataDos.close();
			dataBuffer.close();
			dataBuffer = null;
			dataFileOs.close();

			indexHelper.close();
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

	private void endPostingList() throws IOException {
		indexHelper.endPostingList(buck.blockIdx());
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
	public Bucket getDataBucket() {
		return buck;
	}

	@Override
	public Bucket newDataBucket() {
		buck.reset();
		try {
			if (dataBuffer != null)
				dataBuffer.flush();
			buck.setBlockIdx((int) (dataFileOs.getChannel().position() / Block.BLOCK_SIZE));
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		return buck;
	}

	@Override
	public void moveToDir(File preDir, File dir) {
		File tmpFile = OctreeSSTableWriter.dataFile(preDir, getMeta());
		tmpFile.renameTo(OctreeSSTableWriter.dataFile(dir, getMeta()));
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
		indexHelper.delete(conf.getIndexDir(), meta);
	}
}
