package core.lsmo.common;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
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
import core.lsmo.internformat.InternOctreeSSTableWriter;
import core.lsmo.octree.IOctreeIterator;
import core.lsmo.octree.MemoryOctree;
import core.lsmo.octree.MemoryOctreeIterator;
import core.lsmo.octree.OctreeMerger;
import core.lsmo.octree.OctreeNode;
import core.lsmo.octree.OctreePrepareForWriteVisitor;
import core.lsmt.IMemTable;
import core.lsmt.IMemTable.SSTableMeta;
import core.lsmt.ISSTableReader;
import core.lsmt.ISSTableWriter;
import core.lsmt.WritableComparable;
import util.Configuration;
import util.GroupByKeyIterator;
import util.Pair;
import util.PeekIterDecorate;

public class OctreeSSTableWriter extends ISSTableWriter {
	private static final Logger logger = Logger.getLogger(InternOctreeSSTableWriter.class);

	// used to store the directory that maps keyword to posting list
	BDBBtree dirMap = null;

	FileOutputStream dataFileOs;
	DataOutputStream dataDos;

	FileOutputStream markFileOs;
	DataOutputStream markDos;

	IOctreeWriter indexHelper;

	GroupByKeyIterator<WritableComparable, IOctreeIterator> iter;

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

		iter = new GroupByKeyIterator<WritableComparable, IOctreeIterator>(new Comparator<WritableComparable>() {
			@Override
			public int compare(WritableComparable o1, WritableComparable o2) {
				return o1.compareTo(o2);
			}
		});
		for (final ISSTableReader table : tables) {
			iter.add(PeekIterDecorate.decorate(new SSTableScanner(table)));
		}

		try {
			indexHelper = (IOctreeWriter) Class.forName(conf.getIndexHelper())
					.getConstructor(OctreeSSTableWriter.class).newInstance(this);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public OctreeSSTableWriter(List<IMemTable> tables, Configuration conf) {
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
		try {
			indexHelper = (IOctreeWriter) Class.forName(conf.getIndexHelper())
					.getConstructor(OctreeSSTableWriter.class).newInstance(this);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public OctreeSSTableWriter(SSTableMeta meta, Configuration conf) {
		super(meta, conf);

		try {
			indexHelper = (IOctreeWriter) Class.forName(conf.getIndexHelper())
					.getConstructor(OctreeSSTableWriter.class).newInstance(this);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void open(File dir) throws IOException {
		if (!dir.exists())
			dir.mkdirs();

		dataFileOs = new FileOutputStream(dataFile(dir, meta));
		dataDos = new DataOutputStream(dataFileOs);

		markFileOs = new FileOutputStream(markFile(dir, meta));
		markDos = new DataOutputStream(markFileOs);

		dirMap = new BDBBtree(dirMetaFile(dir, meta), conf);
		dirMap.open(false, false);
	}

	@Override
	public void write() throws IOException {
		while (iter.hasNext()) {
			Entry<WritableComparable, List<IOctreeIterator>> entry = iter.next();
			logger.debug(String.format("writing postinglist for key%s", entry.getKey()));
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
	protected void writeOctree(IOctreeIterator iter) throws IOException {
		int size = 0;
		OctreeNode octreeNode = null;
		while (iter.hasNext()) {
			octreeNode = iter.nextNode();
			if (octreeNode.size() > 0 || Encoding.isMarkupNode(octreeNode.getEncoding())) {
				if (octreeNode.size() > 0) {
					if (shouldSplitOctant(octreeNode)) {
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
			System.err.println(size + " size do not  equals " + iter.getMeta().size);
		}
	}

	@Override
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
	public Bucket getDataBucket() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Bucket newDataBucket() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void moveToDir(File preDir, File dir) {
		try {
			FileUtils.moveDirectory(dataFile(preDir, meta), dataFile(dir, meta));
			FileUtils.moveDirectory(markFile(preDir, meta), markFile(dir, meta));
			FileUtils.moveDirectory(dirMetaFile(preDir, meta), dirMetaFile(dir, meta));
		} catch (IOException e) {
			logger.error(e.getMessage());
		}
	}

	@Override
	public boolean validate(SSTableMeta meta) {
		return dirMetaFile(conf.getIndexDir(), meta).exists() && dataFile(conf.getIndexDir(), meta).exists()
				&& markFile(conf.getIndexDir(), meta).exists();
	}

	@Override
	public void delete(File indexDir, SSTableMeta meta) {
		try {
			FileUtils.deleteDirectory(dataFile(indexDir, meta));
			FileUtils.deleteDirectory(markFile(indexDir, meta));
			FileUtils.deleteDirectory(dirMetaFile(indexDir, meta));
		} catch (IOException e) {
			logger.error(e.getMessage());
		}
	}

	public DataOutputStream getDataDos() {
		return dataDos;
	}

	public DataOutputStream getMarkDos() {
		return markDos;
	}

	public int currentBlockIdx() throws IOException {
		dataDos.flush();
		dataFileOs.flush();
		return (int) (dataFileOs.getChannel().size() / Block.BLOCK_SIZE);
	}

	public int currentMarkBlockIdx() throws IOException {
		markDos.flush();
		markFileOs.flush();
		return (int) (markFileOs.getChannel().size() / Block.BLOCK_SIZE);
	}

	public static File dirMetaFile(File dir, SSTableMeta meta) {
		return new File(dir, String.format("%d_%d_dir", meta.version, meta.level));
	}

	public static File dataFile(File dir, SSTableMeta meta) {
		return new File(dir, String.format("%d_%d.data", meta.version, meta.level));
	}

	public static File markFile(File dir, SSTableMeta meta) {
		return new File(dir, String.format("%d_%d.mark", meta.version, meta.level));
	}

}
