package core.lsmo.internformat;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import core.commom.WritableComparable;
import core.io.Block;
import core.lsmo.MarkDirEntry;
import core.lsmo.persistence.IOctreeSSTableWriter;
import core.lsmo.persistence.SkipCell;
import core.lsmt.DirEntry;
import core.lsmt.IMemTable;
import core.lsmt.IMemTable.SSTableMeta;
import core.lsmt.postinglist.ISSTableReader;
import util.Configuration;

/**
 * 把meta data写入到data文件中
 * 
 * ----------------------------------------------------------------------------
 * metablock blockheader encoding1->boffset+loc3 encoding2->boffset2+loc3
 * encoding3+boffset3+loc3
 * ----------------------------------------------------------------------------
 * datablock blockheader numofoctant octant1 octant2 octant3
 * ----------------------------------------------------------------------------
 * datablock blockheader numofoctant octant1 octant2 octant3
 * ----------------------------------------------------------------------------
 * 
 * 
 * DirEntry应该记录 startoffset, endoffset indexoffset, samplenum
 * 
 * @author xiafan
 */
public class InternOctreeSSTableWriter extends IOctreeSSTableWriter {
	private static final Logger logger = Logger.getLogger(InternOctreeSSTableWriter.class);

	List<DirEntry> dirsStartInCurBuck = new ArrayList<DirEntry>();// 起始于最后一个buck的dirs
	List<DirEntry> dirsEndInCurBuck = new ArrayList<DirEntry>();// 起始于最后一个buck的dirs

	/**
	 * 用于压缩多个磁盘上的Sstable文件，主要是需要得到一个iter
	 * 
	 * @param meta新生成文件的元数据
	 * @param tables
	 *            需要压缩的表
	 * @param conf
	 *            配置信息
	 */
	public InternOctreeSSTableWriter(SSTableMeta meta, List<ISSTableReader> tables, Configuration conf) {
		super(meta, tables, conf);
	}

	@SuppressWarnings({ "rawtypes" })
	public InternOctreeSSTableWriter(List<IMemTable> tables, Configuration conf) {
		super(tables, conf);
	}

	@Override
	public void open(File dir) throws IOException {
		super.open(dir);
		indexCell = new SkipCell(dataBos.currentBlockIdx(), conf.getSecondaryKeyFactory());
		dataBos.appendBlock(indexCell.write(-1));
		curDataBuck.setBlockIdx(dataBos.currentBlockIdx());
	}

	@Override
	public DirEntry startNewPostingList(WritableComparable key) {
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
	public void endNewPostingList() throws IOException {
		curDir.endBucketID.copy(curDataBuck.blockIdx());
		curDir.sampleNum = indexCell.toFileOffset();

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
	protected void flushAndNewSkipCell() throws IOException {
		Block block = indexCell.write(dataBos.currentBlockIdx());
		dataBos.writeBlock(block);
		dataBos.flushAppends();

		indexCell.reset();
		indexCell.setBlockIdx(dataBos.currentBlockIdx());
		dataBos.appendBlock(indexCell.write(-1));

		curDataBuck.setBlockIdx(dataBos.currentBlockIdx());

		for (DirEntry entry : dirsStartInCurBuck) {
			entry.startBucketID.blockID = curDataBuck.blockIdx().blockID;
		}

		for (DirEntry entry : dirsEndInCurBuck) {
			entry.endBucketID.blockID = curDataBuck.blockIdx().blockID;
		}
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
		dataBos.appendBlocks(curDataBuck.toBlocks());
		curDataBuck.reset();
		curDataBuck.setBlockIdx(dataBos.currentBlockIdx());
		// the address of current bucket is fixed now
		dirsStartInCurBuck.clear();
		dirsEndInCurBuck.clear();
	}
}
