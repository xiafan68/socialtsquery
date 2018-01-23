package core.lsmo.bdbformat;

import java.io.IOException;

import core.commom.BDBBTreeBuilder;
import core.commom.BDBBtree;
import core.commom.IndexFileUtils;
import core.commom.Writable;
import core.commom.WritableComparable;
import core.commom.WritableComparable.EncodingFactory;
import core.commom.WritableFactory;
import core.io.Bucket.BucketID;
import core.lsmo.octree.IOctreeIterator;
import core.lsmo.octree.OctreePostingListIter;
import core.lsmo.persistence.SkipCell;
import core.lsmt.IBucketBasedSSTableReader;
import core.lsmt.IMemTable.SSTableMeta;
import util.Configuration;
import util.Pair;

/**
 * This class provides interfaces to locate a posting list given the keyword,
 * locate the position given the octree code and the interface to read the
 * bucket.
 * 
 * @author xiafan
 *
 */
public class DiskSSTableBDBReader extends IBucketBasedSSTableReader {
	private BDBBtree skipListMap;

	public DiskSSTableBDBReader(Configuration conf, SSTableMeta meta) {
		super(conf, meta);
	}

	public IOctreeIterator getPostingListIter(WritableComparable key, int start, int end) throws IOException {
		IOctreeIterator iter = new OctreePostingListIter(getDirEntry(key), this, start, end);
		iter.open();
		return iter;
	}

	@Override
	public void initIndex() throws IOException {
		skipListMap = BDBBTreeBuilder.create().setDir(IndexFileUtils.idxFile(getConf().getIndexDir(), meta))
				.setKeyFactory(getConf().getDirKeyFactory()).setSecondaryKeyFactory(EncodingFactory.INSTANCE)
				.setValueFactory(WritableFactory.SkipCellFactory.INSTANCE).setAllowDuplicates(true).setReadOnly(true)
				.build();
		skipListMap.open();
	}

	@Override
	public Pair<WritableComparable, BucketID> floorOffset(WritableComparable curKey,
			WritableComparable curCode) throws IOException {
		Pair<WritableComparable, Writable> pair = skipListMap.floor(curKey, curCode);
		if (pair == null) {
			return null;
		} else {
			SkipCell cell = ((SkipCell) pair.getValue());
			return cell.getIndexEntry(cell.floorOffset(curKey, 0, cell.size()));
		}
	}

	@Override
	public Pair<WritableComparable, BucketID> cellOffset(WritableComparable curKey, WritableComparable curCode)
			throws IOException {
		Pair<WritableComparable, Writable> pair = skipListMap.floor(curKey, curCode);
		if (pair == null) {
			return null;
		} else {
			SkipCell cell = ((SkipCell) pair.getValue());
			int idx = cell.cellOffset(curKey, 0, cell.size());
			if (idx == -1) {
				pair = skipListMap.cell(curKey, curCode);
				if (pair == null)
					return null;
				else {
					cell = ((SkipCell) pair.getValue());
					return cell.getIndexEntry(0);
				}
			} else
				return cell.getIndexEntry(idx);
		}
	}

	@Override
	public void closeIndex() {
		skipListMap.close();
	}
}
