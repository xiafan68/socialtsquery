package core.lsmo.bdbformat;

import java.io.File;
import java.io.IOException;
import java.util.List;

import core.commom.BDBBTreeBuilder;
import core.commom.BDBBtree;
import core.commom.IndexFileUtils;
import core.commom.WritableComparable;
import core.commom.WritableComparable.EncodingFactory;
import core.commom.WritableFactory;
import core.lsmo.MarkDirEntry;
import core.lsmo.persistence.IOctreeSSTableWriter;
import core.lsmo.persistence.SkipCell;
import core.lsmt.DirEntry;
import core.lsmt.IMemTable;
import core.lsmt.IMemTable.SSTableMeta;
import core.lsmt.postinglist.ISSTableReader;
import util.Configuration;

public class BDBSkipListOctreeSSTableWriter extends IOctreeSSTableWriter {

	private BDBBtree skipListMap;
	int cellIndex = 0;

	@SuppressWarnings("rawtypes")
	public BDBSkipListOctreeSSTableWriter(List<IMemTable> tables, Configuration conf) {
		super(tables, conf);
	}

	public BDBSkipListOctreeSSTableWriter(SSTableMeta meta, List<ISSTableReader> tables, Configuration conf) {
		super(meta, tables, conf);
	}

	@Override
	public void open(File dir) throws IOException {
		super.open(dir);
		indexCell = new SkipCell(cellIndex++, conf.getSecondaryKeyFactory());
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
	protected DirEntry startNewPostingList(WritableComparable key) {
		DirEntry ret;
		if (conf.standaloneSentinal()) {
			ret = new MarkDirEntry();
			((MarkDirEntry) ret).markNum = 0;
		} else {
			ret = new DirEntry();
		}

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
		if (conf.standaloneSentinal()) {
			((MarkDirEntry) curDir).markNum = sentinelOctantNum;
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
		indexCell.setBlockIdx(cellIndex++);
	}

	@Override
	public void endWritingSSTable() throws IOException {
		if (curDataBuck.octNum() > 0) {
			dataBos.appendBlocks(curDataBuck.toBlocks());
			dataBos.flushAppends();
		}

		if (markupBuck != null && markupBuck.octNum() > 0) {
			markupBos.appendBlocks(markupBuck.toBlocks());
			markupBos.flushAppends();
		}
	}

}
