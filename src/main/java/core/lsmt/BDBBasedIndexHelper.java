package core.lsmt;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

import Util.Configuration;
import core.commom.BDBBtree;
import core.io.Bucket.BucketID;
import core.lsmt.IMemTable.SSTableMeta;

public class BDBBasedIndexHelper extends IndexHelper {

	protected BDBBtree dirMap = null;
	protected BDBBtree skipList = null;
	WritableComparableKey curKey = null;

	public BDBBasedIndexHelper(Configuration conf) {
		super(conf);
	}

	public void openIndexFile(File dir, SSTableMeta meta) throws FileNotFoundException {
		if (!dir.exists())
			dir.mkdirs();

		dirMap = new BDBBtree(dirMetaFile(dir, meta), conf);
		dirMap.open(false, false);
		skipList = new BDBBtree(idxFile(dir, meta), conf);
		skipList.open(false, true);
	}

	@Override
	public void startPostingList(WritableComparableKey key, BucketID newListStart) throws IOException {
		super.startPostingList(key, newListStart);
		curKey = key;
		curDir.indexStartOffset = -1;// it is not used in this setup
	}

	@Override
	public void endPostingList(BucketID postingListEnd) throws IOException {
		super.endPostingList(postingListEnd);
		dirMap.insert(curKey, curDir);
	}

	@Override
	public void moveToDir(File preDir, File dir, SSTableMeta meta) {
		File tmpFile = idxFile(preDir, meta);
		tmpFile.renameTo(idxFile(dir, meta));
		tmpFile = dirMetaFile(preDir, meta);
		tmpFile.renameTo(dirMetaFile(dir, meta));
	}

	public void buildIndex(WritableComparableKey code, BucketID id) throws IOException {
		curDir.sampleNum++;
		skipList.insert(curKey, code, id);
	}

	public static File idxFile(File dir, SSTableMeta meta) {
		return new File(dir, String.format("%d_%d_idx", meta.version, meta.level));
	}

	public static File dirMetaFile(File dir, SSTableMeta meta) {
		return new File(dir, String.format("%d_%d_dir", meta.version, meta.level));
	}

	@Override
	public void close() throws IOException {
		if (dirMap != null) {
			dirMap.close();
			dirMap = null;
			skipList.close();
		}
	}

	@Override
	public boolean delete(File indexDir, SSTableMeta meta) {
		boolean ret = dirMetaFile(indexDir, meta).delete();
		ret = ret && idxFile(indexDir, meta).delete();
		return ret;
	}

	@Override
	public boolean validate(SSTableMeta meta) {
		File idxFile = idxFile(conf.getIndexDir(), meta);
		File dirFile = dirMetaFile(conf.getIndexDir(), meta);
		if (!idxFile.exists() || !dirFile.exists()) {
			return false;
		}
		return true;
	}
}