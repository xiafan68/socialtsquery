package core.lsmi;

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
import org.apache.log4j.Logger;

import collection.CollectionUtils;
import common.MidSegment;
import core.commom.BDBBTreeBuilder;
import core.commom.BDBBtree;
import core.commom.IndexFileUtils;
import core.commom.WritableComparable;
import core.io.Block;
import core.io.Bucket;
import core.io.Bucket.BucketID;
import core.lsmi.SortedListMemTable.SortedListPostinglist;
import core.lsmt.DirEntry;
import core.lsmt.IMemTable;
import core.lsmt.IMemTable.SSTableMeta;
import core.lsmt.postinglist.IPostingListIterator;
import core.lsmt.postinglist.ISSTableReader;
import core.lsmt.postinglist.ISSTableWriter;
import util.Configuration;
import util.GroupByKeyIterator;
import util.Pair;
import util.PeekIterDecorate;

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

	GroupByKeyIterator<WritableComparable, IPostingListIterator> view = new GroupByKeyIterator<WritableComparable, IPostingListIterator>(
			WritableComparable.WritableComparableKeyComp.INSTANCE);;

	FileOutputStream dataFileOs;
	DataOutputStream dataDos;
	private int step;
	Bucket buck = new Bucket(-1);

	BDBBtree dirMap = null;
	DirEntry curDir;

	public SortedListSSTableWriter(List<IMemTable> tables, Configuration conf) {
		super(null, conf);
		int version = 0;

		for (final IMemTable<SortedListPostinglist> table : tables) {
			version = Math.max(version, table.getMeta().version);
			view.add(PeekIterDecorate.decorate(new Iterator<Entry<WritableComparable, IPostingListIterator>>() {
				Iterator<WritableComparable> keyIter = table.getReader().keySetIter();

				@Override
				public boolean hasNext() {
					return keyIter.hasNext();
				}

				@Override
				public Entry<WritableComparable, IPostingListIterator> next() {
					WritableComparable key = keyIter.next();
					Entry<WritableComparable, IPostingListIterator> ret = null;
					try {
						ret = new Pair<WritableComparable, IPostingListIterator>(key,
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
		this.step = conf.getIndexStep();
	}

	public SortedListSSTableWriter(SSTableMeta meta, List<ISSTableReader> tables, Configuration conf) {
		super(meta, conf);

		for (final ISSTableReader table : tables) {
			view.add(PeekIterDecorate.decorate(new Iterator<Entry<WritableComparable, IPostingListIterator>>() {
				ISSTableReader reader = table;
				Iterator<WritableComparable> iter = table.keySetIter();

				@Override
				public boolean hasNext() {
					return iter.hasNext();
				}

				@Override
				public Entry<WritableComparable, IPostingListIterator> next() {
					WritableComparable entry = iter.next();
					Pair<WritableComparable, IPostingListIterator> ret;
					try {
						ret = new Pair<WritableComparable, IPostingListIterator>(entry,
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
		this.step = conf.getIndexStep();
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

	public void startPostingList(WritableComparable key) {
		curDir = new DirEntry();
		curDir.curKey = key;
		curDir.sampleNum = 0;
		curDir.size = 0;
		curDir.minTime = Integer.MAX_VALUE;
		curDir.maxTime = Integer.MIN_VALUE;
	}

	@Override
	public void open(File dir) throws IOException {
		if (!dir.exists())
			dir.mkdirs();

		dataFileOs = new FileOutputStream(IndexFileUtils.dataFile(dir, meta));
		dataDos = new DataOutputStream(dataFileOs);
		dirMap = BDBBTreeBuilder.create().setDir(IndexFileUtils.dirMetaFile(conf.getIndexDir(), meta))
				.setKeyFactory(conf.getDirKeyFactory()).setValueFactory(conf.getDirValueFactory())
				.setAllowDuplicates(false).setReadOnly(false).build();
		dirMap.open();
	}

	@Override
	public void write() throws IOException {
		while (view.hasNext()) {
			Entry<WritableComparable, List<IPostingListIterator>> pair = view.next();
			List<Iterator<MidSegment>> list = new ArrayList<Iterator<MidSegment>>();
			for (IPostingListIterator iter : pair.getValue()) {
				list.add(new PostingListDecorator(iter));
			}
			Iterator<MidSegment> iter = CollectionUtils.merge(list, new Comparator<MidSegment>() {
				@Override
				public int compare(MidSegment o1, MidSegment o2) {
					return o1.compareTo(o2);
				}

			});
			writePostingList(new WriterHelper(step), pair.getKey(), iter);
		}
	}

	private static class WriterHelper {
		boolean first = true;
		SubList list;

		public WriterHelper(int sizeLimit) {
			list = new SubList(sizeLimit);
		}

		public void init() {
			list.init();
		}
	}

	private void writePostingList(WriterHelper helper, WritableComparable key, Iterator<MidSegment> iter)
			throws IOException {
		startPostingList(key);
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
		endPostingList(buck.blockIdx());
	}

	private void endPostingList(BucketID blockIdx) throws IOException {
		curDir.endBucketID.copy(blockIdx);
		dirMap.insert(curDir.curKey, curDir);
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
				newDataBucket();
			} else if (!buck.canStore(data.length)) {
				buck.write(dataDos);
				newDataBucket();
			}
			buck.storeOctant(data);
			if (helper.first) {
				helper.first = false;
				curDir.startBucketID.copy(buck.blockIdx());
			}
			helper.init();
		}
	}

	@Override
	public void moveToDir(File preDir, File dir) {
		File tmpFile = IndexFileUtils.dataFile(preDir, getMeta());
		tmpFile.renameTo(IndexFileUtils.dataFile(dir, getMeta()));

		tmpFile = IndexFileUtils.dirMetaFile(preDir, getMeta());
		tmpFile.renameTo(IndexFileUtils.dirMetaFile(dir, getMeta()));
	}

	@Override
	public void close() throws IOException {
		if (buck != null) {
			buck.write(dataDos);
			logger.debug("last bucket:" + buck.toString());
		}

		dataFileOs.close();
		dataDos.close();

		dirMap.close();
	}

	public Bucket newDataBucket() {
		buck.reset();
		try {
			buck.setBlockIdx((int) (dataFileOs.getChannel().position() / Block.BLOCK_SIZE));
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		return buck;
	}

	@Override
	public boolean validate(SSTableMeta meta) {
		File dFile = IndexFileUtils.dataFile(conf.getIndexDir(), meta);
		File metaFile = IndexFileUtils.dirMetaFile(conf.getIndexDir(), meta);
		return dFile.exists() && metaFile.exists();
	}

	@Override
	public void delete(File indexDir, SSTableMeta meta) throws IOException {
		if (!IndexFileUtils.dataFile(conf.getIndexDir(), meta).delete()) {
			throw new IOException(String.format("file %s can not be deleted",
					IndexFileUtils.dataFile(conf.getIndexDir(), meta).getAbsolutePath()));
		}
		FileUtils.deleteDirectory(IndexFileUtils.dirMetaFile(conf.getIndexDir(), meta));
	}
}
