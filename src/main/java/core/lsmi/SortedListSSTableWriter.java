package core.lsmi;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.log4j.Logger;

import xiafan.util.collection.CollectionUtils;
import Util.GroupByKeyIterator;
import Util.Pair;
import common.MidSegment;
import core.io.Bucket;
import core.io.Bucket.BucketID;
import core.lsmo.OctreeSSTableWriter;
import core.lsmt.IMemTable.SSTableMeta;
import core.lsmt.IPostingListIterator;
import core.lsmt.ISSTableWriter;
import fanxia.file.ByteUtil;

/**
 * dirMeta, index, datafile
 * 
 * dataFile:
 * bucket
 * bucket
 * bucket
 * TODO:
 * enable mixture of posting list?
 * @author xiafan
 *
 */
public class SortedListSSTableWriter extends ISSTableWriter {
	private static final Logger logger = Logger
			.getLogger(SortedListSSTableWriter.class);

	GroupByKeyIterator<Integer, IPostingListIterator> view;
	SSTableMeta meta;

	FileOutputStream dataFileOs;
	DataOutputStream dataDos;
	FileOutputStream indexFileDos;
	DataOutputStream indexDos; // write down the encoding of each block
	DataOutputStream dirDos; // write directory meta
	private int step;

	DirEntry curDir = new DirEntry();
	Bucket buck = new Bucket(0);

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
		while (view.hasNext()) {
			Entry<Integer, List<IPostingListIterator>> pair = view.next();
			List<Iterator<MidSegment>> list = new ArrayList<Iterator<MidSegment>>();
			for (IPostingListIterator iter : pair.getValue()) {
				list.add(new PostingListDecorator(iter));
			}
			Iterator<MidSegment> iter = CollectionUtils.merge(list);
			writePostingList(pair.getKey(), iter);
		}
	}

	private void writePostingList(Integer key, Iterator<MidSegment> iter)
			throws IOException {
		curDir.init();
		curDir.curKey = key;
		MidSegment cur = null;
		int curSize = 0;
		int preTop = Integer.MIN_VALUE;
		int curStep = 0;
		while (cur != null || iter.hasNext()) {
			// write one buck
			if (buck.available() < 4) {
				newBucket();
			}
			ByteArrayOutputStream out = new ByteArrayOutputStream(
					buck.available());
			DataOutputStream output = new DataOutputStream(out);
			try {
				output.writeInt(0);// record the number segs
			} catch (IOException e1) {
			}

			// fill current bucket
			while (cur != null || iter.hasNext()) {
				int preSize = output.size();
				if (cur == null)
					cur = iter.next();
				try {
					cur.write(output);
				} catch (IOException e) {
				}
				if (output.size() > buck.available()) {
					byte[] data = Arrays.copyOf(out.toByteArray(), preSize);
					ByteUtil.writeInt(curSize, data, 0);
					buck.storeOctant(data);
					newBucket();
					break;
				} else {
					if (cur.getPoint().getZ() != preTop) {
						curStep++;
						if (curStep > step) {
							curStep = 0;
							buildIndex(preTop, buck.blockIdx());
						}
					}
					curSize++;
				}
			}
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
		return new File(dir, String.format("%d_%d.data", meta.version,
				meta.level));
	}

	public static File idxFile(File dir, SSTableMeta meta) {
		return new File(dir, String.format("%d_%d.idx", meta.version,
				meta.level));
	}

	public static File dirMetaFile(File dir, SSTableMeta meta) {
		return new File(dir, String.format("%d_%d.meta", meta.version,
				meta.level));
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
	 * @param key
	 * @param id
	 * @throws IOException
	 */
	public void buildIndex(int key, BucketID id) throws IOException {
		curDir.sampleNum++;
		indexDos.writeInt(key);
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
