package core.lsmo.bdbformat;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;

import core.commom.BDBBtree;
import core.commom.WritableComparableKey;
import core.io.Bucket;
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

		skipListMap = new BDBBtree(skipListDir(dir, meta), conf);
		skipListMap.open(false, false);
	}

	@Override
	public void close() throws IOException {
		super.close();
		skipListMap.close();
	}

	@Override
	protected DirEntry startNewPostingList(WritableComparableKey key) {
		MarkDirEntry ret = new MarkDirEntry(conf.getIndexKeyFactory());

		ret.curKey = key;
		ret.sampleNum = 0;
		ret.size = 0;
		ret.minTime = Integer.MAX_VALUE;
		ret.maxTime = Integer.MIN_VALUE;
		return null;
	}

	@Override
	protected void endNewPostingList() {
		// TODO Auto-generated method stub

	}

	@Override
	protected void newSentinalBucket() {
		// TODO Auto-generated method stub

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
	public Bucket getDataBucket() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void newDataBucket() throws IOException {
		curDataBuck.reset();
		curDataBuck.setBlockIdx(dataBos.currentBlockIdx());
	}

	public static File skipListDir(File dir, SSTableMeta meta) {
		return new File(dir, String.format("%d_%d_skip", meta.version, meta.level));
	}

	@Override
	protected void flushAndNewSkipCell() {
		skipListMap
	}

}
