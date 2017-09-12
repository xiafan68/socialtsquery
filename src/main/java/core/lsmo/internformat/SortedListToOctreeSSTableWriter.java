package core.lsmo.internformat;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import common.MidSegment;
import core.io.Bucket;
import core.lsmi.SortedListMemTable.SortedListPostinglist;
import core.lsmo.octree.MemoryOctree;
import core.lsmo.octree.MemoryOctreeIterator;
import core.lsmt.IMemTable;
import core.lsmt.IMemTable.SSTableMeta;
import core.lsmt.ISSTableReader;
import core.lsmt.ISSTableWriter;
import core.lsmt.PostingListMeta;
import core.lsmt.WritableComparable;
import util.Configuration;

public class SortedListToOctreeSSTableWriter extends ISSTableWriter {
	// private static final Logger logger =
	// Logger.getLogger(SortedListSSTableWriter.class);

	InternOctreeSSTableWriter octreeWriter;
	Iterator<Entry<WritableComparable, SortedListPostinglist>> iter;

	public SortedListToOctreeSSTableWriter(List<IMemTable> tables, Configuration conf) {
		super(null, conf);
		iter = tables.get(0).iterator();
		SSTableMeta meta = tables.get(0).getMeta();
		octreeWriter = new InternOctreeSSTableWriter(new SSTableMeta(meta.version, meta.level), conf);
	}

	/**
	 * !!!不能再compact中使用
	 * 
	 * @param meta
	 * @param tables
	 * @param conf
	 */
	public SortedListToOctreeSSTableWriter(SSTableMeta meta, List<ISSTableReader> tables, Configuration conf) {
		super(meta, conf);
		throw new RuntimeException(this.getClass().toString() + " doesn't support compact");
	}

	@Override
	public SSTableMeta getMeta() {
		return octreeWriter.getMeta();
	}

	@Override
	public void open(File dir) throws IOException {
		octreeWriter.open(dir);
	}

	@Override
	public void write() throws IOException {
		while (iter.hasNext()) {
			Entry<WritableComparable, SortedListPostinglist> pair = iter.next();
			Iterator<MidSegment> iter = pair.getValue().iterator();
			// build the octree
			MemoryOctree tree = new MemoryOctree(new PostingListMeta(), conf.getOctantSizeLimit());
			while (iter.hasNext()) {
				tree.insert(iter.next());
			}
			// write using octree writer
			octreeWriter.startNewPostingList(pair.getKey());
			octreeWriter.updateMeta(tree.getMeta());
			octreeWriter.writeOctree(new MemoryOctreeIterator(tree));
			octreeWriter.endPostingList();
		}
	}

	@Override
	public void moveToDir(File preDir, File dir) {
		octreeWriter.moveToDir(preDir, dir);
	}

	@Override
	public void close() throws IOException {
		octreeWriter.close();
	}

	@Override
	public boolean validate(SSTableMeta meta) {
		return octreeWriter.validate(meta);
	}

	@Override
	public void delete(File indexDir, SSTableMeta meta) {
		octreeWriter.delete(indexDir, meta);
	}

	@Override
	public Bucket getDataBucket() {
		return null;
	}

	@Override
	public Bucket newDataBucket() {
		return null;
	}
}
