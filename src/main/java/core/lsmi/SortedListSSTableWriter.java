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

import org.apache.log4j.Logger;

import collection.CollectionUtils;
import common.MidSegment;
import core.commom.WritableComparableKey;
import core.io.Block;
import core.io.Bucket;
import core.lsmi.SortedListMemTable.SortedListPostinglist;
import core.lsmo.bdbformat.OctreeSSTableWriter;
import core.lsmt.IMemTable;
import core.lsmt.IMemTable.SSTableMeta;
import core.lsmt.IndexHelper;
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

	GroupByKeyIterator<WritableComparableKey, IPostingListIterator> view = new GroupByKeyIterator<WritableComparableKey, IPostingListIterator>(
			WritableComparableKey.WritableComparableKeyComp.INSTANCE);;

	FileOutputStream dataFileOs;
	DataOutputStream dataDos;
	private int step;
	Bucket buck = new Bucket(-1);
	IndexHelper indexHelper;

	public SortedListSSTableWriter(List<IMemTable> tables, Configuration conf) {
		super(null, conf);
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
		this.step = conf.getIndexStep();
		try {
			indexHelper = (IndexHelper) Class.forName(conf.getIndexHelper()).getConstructor(Configuration.class)
					.newInstance(conf);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public SortedListSSTableWriter(SSTableMeta meta, List<ISSTableReader> tables, Configuration conf) {
		super(meta, conf);

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
		this.step = conf.getIndexStep();
		try {
			indexHelper = (IndexHelper) Class.forName(conf.getIndexHelper()).getConstructor(Configuration.class)
					.newInstance(conf);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
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
	public void open(File dir) throws IOException {
		if (!dir.exists())
			dir.mkdirs();

		dataFileOs = new FileOutputStream(dataFile(dir, meta));
		dataDos = new DataOutputStream(dataFileOs);
		indexHelper.openIndexFile(dir, meta);
	}

	@Override
	public void write() throws IOException {
		while (view.hasNext()) {
			Entry<WritableComparableKey, List<IPostingListIterator>> pair = view.next();
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
		indexHelper.startPostingList(key, null);
		MidSegment seg = null;
		while (iter.hasNext()) {
			seg = iter.next();
			helper.list.addSegment(seg);
			indexHelper.process(seg.getStart(), seg.getEndTime());

			if (helper.list.isFull()) {
				writeSubList(helper);
			}
		}
		writeSubList(helper);
		indexHelper.endPostingList(buck.blockIdx());
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
				indexHelper.setupDataStartBlockIdx(buck.blockIdx());
			}
			helper.init();
		}

	}

	@Override
	public void moveToDir(File preDir, File dir) {
		indexHelper.moveToDir(preDir, dir, meta);
		File tmpFile = OctreeSSTableWriter.dataFile(preDir, getMeta());
		tmpFile.renameTo(OctreeSSTableWriter.dataFile(dir, getMeta()));
	}

	public static File dataFile(File dir, SSTableMeta meta) {
		return new File(dir, String.format("%d_%d.data", meta.version, meta.level));
	}

	@Override
	public void close() throws IOException {
		if (buck != null) {
			buck.write(dataDos);
			logger.debug("last bucket:" + buck.toString());
		}

		dataFileOs.close();
		dataDos.close();

		indexHelper.close();
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
