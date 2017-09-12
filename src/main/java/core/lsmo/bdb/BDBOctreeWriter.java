package core.lsmo.bdb;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;

import core.commom.BDBBtree;
import core.io.Block;
import core.io.Bucket;
import core.lsmo.common.IOctreeWriter;
import core.lsmo.common.OctreeSSTableWriter;
import core.lsmo.common.SkipCell;
import core.lsmt.IMemTable.SSTableMeta;

/**
 * This class use BDB to store skip list. The key is the combination of keyword
 * and encoding. The value is the <code>SkipCell</code>
 * 
 * @author xiafan
 *
 */
public class BDBOctreeWriter extends IOctreeWriter {
	BDBBtree skipListIndex = null;

	public BDBOctreeWriter(OctreeSSTableWriter writer) {
		super(writer);
	}

	@Override
	public int currentMarkIdx() throws IOException {
		return writer.currentMarkBlockIdx();
	}

	@Override
	public void firstDataBuckWritten() throws IOException {
		// do we need this?
		curDir.startBucketID.copy(dataBuck.blockIdx());
		curStep = 0;
		writeFirstBlock = false;
	}

	@Override
	public void flushAndNewSkipCell() throws IOException {
		// first write the meta data
		// store the skipcell in bdb
		skipListIndex.insertSkipCell(curDir.curKey, cell);
		cell.reset();
	}

	@Override
	public void flushAndNewDataBuck() throws IOException {
		dataBuck.write(writer.getDataDos());
		dataBuck.reset();
		dataBuck.setBlockIdx(writer.currentBlockIdx());
	}

	@Override
	public void flushAndNewMarkBuck() throws IOException {
		markUpBuck.write(writer.getMarkDos());
		writer.getMarkDos().flush();
		markUpBuck.reset();
		markUpBuck.setBlockIdx(writer.currentMarkBlockIdx());
	}

	@Override
	public void openIndexFile(File dir, SSTableMeta meta) throws IOException {
		skipListIndex = new BDBBtree(skipIndexFile(dir, meta), conf);
		skipListIndex.open(false, true);

		cell = new SkipCell(writer.currentBlockIdx(), conf.getIndexValueFactory());
		dataBuck = new Bucket(writer.currentBlockIdx());
		markUpBuck = new Bucket(currentMarkIdx() * Block.BLOCK_SIZE);
	}

	private static File skipIndexFile(File dir, SSTableMeta meta) {
		return new File(dir, String.format("%d_%d_skip", meta.version, meta.level));
	}

	@Override
	public void moveToDir(File preDir, File dir, SSTableMeta meta) {
		try {
			FileUtils.moveDirectory(skipIndexFile(preDir, meta), skipIndexFile(dir, meta));
		} catch (IOException e) {
		}
	}

	@Override
	public void close() throws IOException {
		skipListIndex.close();
	}

	@Override
	public boolean delete(File indexDir, SSTableMeta meta) {
		try {
			FileUtils.deleteDirectory(skipIndexFile(indexDir, meta));
		} catch (IOException e) {
			return false;
		}
		return true;
	}

}
