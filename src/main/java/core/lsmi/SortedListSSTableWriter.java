package core.lsmi;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import org.apache.commons.io.output.ByteArrayOutputStream;

import xiafan.util.collection.CollectionUtils;
import Util.GroupByKeyIterator;
import common.MidSegment;
import core.commom.Encoding;
import core.io.Bucket;
import core.io.Bucket.BucketID;
import core.lsmo.OctreeSSTableWriter;
import core.lsmt.IMemTable.SSTableMeta;
import core.lsmt.IPostingListIterator;
import core.lsmt.ISSTableWriter;

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
	GroupByKeyIterator<Integer, IPostingListIterator> view;

	@Override
	public SSTableMeta getMeta() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void write(File dir) throws IOException {
		ByteArrayOutputStream output = new ByteArrayOutputStream(getBucket()
				.available());

		while (view.hasNext()) {
			Entry<Integer, List<IPostingListIterator>> pair = view.next();
			Iterator<MidSegment> iter = CollectionUtils.merge(pair.getValue());
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
		// TODO Auto-generated method stub

	}

	/**
	 * 这个实现目前记录的是某个max value对应的bucket offset
	 * @param key
	 * @param id
	 * @throws IOException
	 */
	public void addSample(int key, BucketID id) throws IOException {
		curDir.sampleNum++;
		code.write(indexDos);
		id.write(indexDos);
	}

	@Override
	public Bucket getBucket() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Bucket newBucket() {
		// TODO Auto-generated method stub
		return null;
	}

}
