package core.lsmo.bdbformat;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;

import core.commom.BDBBTreeBuilder;
import core.commom.BDBBtree;
import core.commom.IndexFileUtils;
import core.commom.WritableComparableKey;
import core.commom.WritableComparableKey.EncodingFactory;
import core.commom.WritableFactory;
import core.lsmo.MarkDirEntry;
import core.lsmo.persistence.IOctreeSSTableWriter;
import core.lsmt.DirEntry;
import core.lsmt.IMemTable;
import core.lsmt.IMemTable.SSTableMeta;
import util.Configuration;

public class BDBSkipListOctreeSSTableWriter extends IOctreeSSTableWriter {

	private BDBBtree skipListMap;

	public BDBSkipListOctreeSSTableWriter(List<IMemTable> tables, Configuration conf) {
		super(tables, conf);
	}

	@Override
	public void open(File dir) throws FileNotFoundException {
		super.open(dir);

		skipListMap = BDBBTreeBuilder.create().setDir(IndexFileUtils.idxFile(conf.getIndexDir(), meta))
				.setKeyFactory(conf.getDirKeyFactory()).setSecondaryKeyFactory(EncodingFactory.INSTANCE)
				.setValueFactory(WritableFactory.SkipCellFactory.INSTANCE).setAllowDuplicates(true).setReadOnly(false)
				.build();
		skipListMap.open();
	}

	@Override
	public void close() throws IOException {
		super.close();
		skipListMap.close();
	}

	@Override
	protected DirEntry startNewPostingList(WritableComparableKey key) {
		MarkDirEntry ret = new MarkDirEntry();
		ret.curKey = key;
		ret.sampleNum = 0;
		ret.size = 0;
		ret.minTime = Integer.MAX_VALUE;
		ret.maxTime = Integer.MIN_VALUE;
		return ret;
	}

	@Override
	protected void endNewPostingList() throws IOException {
		// record end positions of posting list, no need to flush markupBuck and
		// curDataBuck as they can be spanned by posting lists
		curDir.endBucketID.copy(curDataBuck.blockIdx());
		if (conf.standaloneSentinal() && sentinelOctantNum > 0) {
			((MarkDirEntry) curDir).endBucketID.copy(markupBuck.blockIdx());
		} else if (conf.standaloneSentinal()) {
			((MarkDirEntry) curDir).endBucketID.blockID = -1;
		}

		dirMap.insert(curDir.curKey, curDir);
	}

	@Override
	protected void flushAndNewSentinalBucket() throws IOException {
		markupBos.writeBlocks(markupBuck.toBlocks());
		markupBuck.reset();
		markupBuck.setBlockIdx(markupBos.currentBlockIdx());
	}

	@Override
	protected void firstLeafOctantWritten() {
		if (conf.standaloneSentinal() || curDir.startBucketID.blockID != -1) {
			curDir.startBucketID.copy(curDataBuck.blockIdx());
		}
	}

	@Override
	protected void firstSentinelOctantWritten() {
		if (conf.standaloneSentinal()) {
			((MarkDirEntry) curDir).startMarkOffset.copy(markupBuck.blockIdx());
		} else if (curDir.startBucketID.blockID != -1) {
			curDir.startBucketID.copy(curDataBuck.blockIdx());
		}
	}

	@Override
	public void flushAndNewDataBucket() throws IOException {
		dataBos.writeBlocks(curDataBuck.toBlocks());
		curDataBuck.reset();
		curDataBuck.setBlockIdx(dataBos.currentBlockIdx());
	}

	public static File skipListDir(File dir, SSTableMeta meta) {
		return new File(dir, String.format("%d_%d_skip", meta.version, meta.level));
	}

	@Override
	protected void flushAndNewSkipCell() throws IOException {
		skipListMap.insert(curDir.curKey, indexCell.getIndexEntry(0).getKey(), indexCell);
		indexCell.reset();
	}

}
