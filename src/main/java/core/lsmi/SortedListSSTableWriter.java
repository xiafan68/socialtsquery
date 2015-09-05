package core.lsmi;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import org.apache.log4j.Logger;

import xiafan.util.collection.CollectionUtils;
import Util.Configuration;
import Util.GroupByKeyIterator;
import Util.Pair;
import Util.PeekIterDecorate;

import common.MidSegment;

import core.io.Bucket;
import core.io.Bucket.BucketID;
import core.lsmi.ListDiskSSTableReader.SegListKey;
import core.lsmi.SortedListMemTable.SortedListPostinglist;
import core.lsmo.OctreeSSTableWriter;
import core.lsmt.IMemTable;
import core.lsmt.IMemTable.SSTableMeta;
import core.lsmt.IPostingListIterator;
import core.lsmt.ISSTableReader;
import core.lsmt.ISSTableWriter;
import core.lsmt.WritableComparableKey;

/**
 * dirMeta, index, datafile
 * 
 * dataFile: bucket bucket bucket TODO: enable mixture of posting list?
 * 
 * @author xiafan
 *
 */
public class SortedListSSTableWriter extends ISSTableWriter {
	private static final Logger logger = Logger.getLogger(SortedListSSTableWriter.class);

	GroupByKeyIterator<WritableComparableKey, IPostingListIterator> view = new GroupByKeyIterator<WritableComparableKey, IPostingListIterator>(
			WritableComparableKey.WritableComparableKeyComp.INSTANCE);;
	SSTableMeta meta;

	FileOutputStream dataFileOs;
	DataOutputStream dataDos;
	FileOutputStream indexFileDos;
	DataOutputStream indexDos; // write down the encoding of each block
	DataOutputStream dirDos; // write directory meta
	private int step;

	DirEntry curDir;
	Bucket buck = new Bucket(-1);
	Configuration conf;

	public SortedListSSTableWriter(List<IMemTable> tables, Configuration conf) {
		this.conf = conf;
		int version = 0;

		for (final IMemTable<SortedListPostinglist> table : tables) {
			version = Math.max(version, table.getMeta().version);
			view.add(PeekIterDecorate.decorate(new Iterator<Entry<WritableComparableKey, IPostingListIterator>>() {
				Iterator<WritableComparableKey> keyIter = table.getReader().keySetIter();

				@Override
				public boolean hasNext() {
					return keyIter.hasNext();
				}

				@Override
				public Entry<WritableComparableKey, IPostingListIterator> next() {
					WritableComparableKey key = keyIter.next();
					Entry<WritableComparableKey, IPostingListIterator> ret = null;
					try {
						ret = new Pair<WritableComparableKey, IPostingListIterator>(key,
								table.getReader().getPostingListScanner(key));
					} catch (IOException e) {
						throw new RuntimeException(e);
					}
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

	public SortedListSSTableWriter(SSTableMeta meta, List<ISSTableReader> tables, Configuration conf) {
		this.conf = conf;
		curDir = new DirEntry(conf.getMemTableKey());
		this.meta = meta;
		for (final ISSTableReader table : tables) {
			view.add(PeekIterDecorate.decorate(new Iterator<Entry<WritableComparableKey, IPostingListIterator>>() {
				ISSTableReader reader = table;
				Iterator<WritableComparableKey> iter = table.keySetIter();

				@Override
				public boolean hasNext() {
					return iter.hasNext();
				}

				@Override
				public Entry<WritableComparableKey, IPostingListIterator> next() {
					WritableComparableKey entry = iter.next();
					Pair<WritableComparableKey, IPostingListIterator> ret;
					try {
						ret = new Pair<WritableComparableKey, IPostingListIterator>(entry,
								reader.getPostingListScanner(entry));
					} catch (IOException e) {
						e.printStackTrace();
						ret = null;
					}
					return ret;
				}

				@Override
				public void remove() {

				}

			}));
		}
		this.step = step;
	}

	@Override
	public SSTableMeta getMeta() {
		return meta;
	}

	static class PostingListDecorator implements Iterator<MidSegment> {
		IPostingListIterator iter;

		public PostingListDecorator(IPostingListIterator iter) {
			this.iter = iter;
		}

		@Override
		public boolean hasNext() {
			try {
				return iter.hasNext();
			} catch (IOException e) {
				return false;
			}
		}

		@Override
		public MidSegment next() {
			Pair<Integer, List<MidSegment>> cur;
			try {
				cur = iter.next();
				return cur.getValue().get(0);
			} catch (IOException e) {
			}
			return null;
		}

		@Override
		public void remove() {

		}

	}

	@Override
	public void write(File dir) throws IOException {
		if (!dir.exists())
			dir.mkdirs();

		dataFileOs = new FileOutputStream(dataFile(dir, meta));
		dataDos = new DataOutputStream(dataFileOs);
		indexFileDos = new FileOutputStream(idxFile(dir, meta));
		indexDos = new DataOutputStream(indexFileDos);
		dirDos = new DataOutputStream(new FileOutputStream(dirMetaFile(dir, meta)));

		while (view.hasNext()) {
			Entry<WritableComparableKey, List<IPostingListIterator>> pair = view.next();
			List<Iterator<MidSegment>> list = new ArrayList<Iterator<MidSegment>>();
			for (IPostingListIterator iter : pair.getValue()) {
				list.add(new PostingListDecorator(iter));
			}
			Iterator<MidSegment> iter = CollectionUtils.merge(list);
			writePostingList(new WriterHelper(step), pair.getKey(), iter);
		}
	}

	private static class WriterHelper {
		boolean first = true;
		SubList list;
		int numOfRecs = 0;

		public WriterHelper(int sizeLimit) {
			list = new SubList(sizeLimit);
		}

		public void init() {
			list.init();
		}
	}

	private void writePostingList(WriterHelper helper, WritableComparableKey key, Iterator<MidSegment> iter)
			throws IOException {
		curDir.init();
		curDir.curKey = key;
		MidSegment seg = null;
		while (iter.hasNext()) {
			seg = iter.next();
			helper.list.addSegment(seg);
			curDir.size++;
			curDir.minTime = Math.min(curDir.minTime, seg.getStart());
			curDir.maxTime = Math.max(curDir.maxTime, seg.getEndTime());
			if (helper.list.isFull()) {
				writeSubList(helper);
			}
		}
		writeSubList(helper);
		curDir.endBucketID.copy(buck.blockIdx());
		curDir.write(dirDos);
	}

	/**
	 * write the sublist to a block
	 * 
	 * @param helper
	 * @throws IOException
	 */
	private void writeSubList(WriterHelper helper) throws IOException {
		if (!helper.list.isEmpty()) {
			byte[] data = helper.list.toByteArray();
			if (buck.blockIdx().blockID < 0) {
				newBucket();
			} else if (!buck.canStore(data.length)) {
				buck.write(dataDos);
				newBucket();
			}
			buck.storeOctant(data);
			if (helper.first) {
				helper.first = false;
				curDir.startBucketID = buck.blockIdx();
				curDir.indexStartOffset = indexFileDos.getChannel().position();
			}
			MidSegment cur = helper.list.get(0);
			buildIndex(new SegListKey(cur.getPoint().getZ(), cur.getStart(), cur.mid), buck.blockIdx());
			helper.init();
		}

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

	public static File dataFile(File dir, SSTableMeta meta) {
		return new File(dir, String.format("%d_%d.data", meta.version, meta.level));
	}

	public static File idxFile(File dir, SSTableMeta meta) {
		return new File(dir, String.format("%d_%d.idx", meta.version, meta.level));
	}

	public static File dirMetaFile(File dir, SSTableMeta meta) {
		return new File(dir, String.format("%d_%d.meta", meta.version, meta.level));
	}

	@Override
	public void close() throws IOException {
		if (buck != null) {
			buck.write(dataDos);
			logger.debug("last bucket:" + buck.toString());
		}

		dataFileOs.close();
		dataDos.close();
		indexFileDos.close();
		indexDos.close();
		dirDos.close();
	}

	/**
	 * 这个实现目前记录的是某个max value对应的bucket offset
	 * 
	 * @param key
	 * @param id
	 * @throws IOException
	 */
	public void buildIndex(SegListKey key, BucketID id) throws IOException {
		curDir.sampleNum++;
		key.write(indexDos);
		id.write(indexDos);
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
