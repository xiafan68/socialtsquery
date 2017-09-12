package core.lsmo.bdbformat;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

import core.commom.BDBBtree;
import core.io.Bucket.BucketID;
import core.lsmt.IMemTable.SSTableMeta;
import util.Configuration;
import core.lsmt.IPostingListerWriter;
import core.lsmt.ISSTableWriter.DirEntry;
import core.lsmt.WritableComparable;

/**
 * The IndexHelpler implementation based on Berkeley DB.
 * 
 * @author xiafan
 *
 */
public class BDBBasedIndexHelper extends IPostingListerWriter {
	private static final Logger logger = Logger.getLogger(BDBBasedIndexHelper.class);
	protected BDBBtree dirMap = null;
	protected BDBBtree skipList = null;
	WritableComparable curKey = null;

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
	public void startPostingList(WritableComparable key, BucketID newListStart) throws IOException {
		super.startPostingList(key, newListStart);
		curKey = key;
		curDir.indexStartOffset = -1;// it is not used in this setup
	}

	@Override
	public DirEntry endPostingList(BucketID postingListEnd) throws IOException {
		super.endPostingList(postingListEnd);
		dirMap.insert(curKey, curDir);
		return curDir;
	}

	@Override
	public void moveToDir(File preDir, File dir, SSTableMeta meta) {
		try {
			FileUtils.moveDirectory(idxFile(preDir, meta), idxFile(dir, meta));
			FileUtils.moveDirectory(dirMetaFile(preDir, meta), dirMetaFile(dir, meta));
		} catch (IOException e) {
			logger.error(e.getMessage());
		}

	}

	public void buildIndex(WritableComparable code, BucketID id) throws IOException {
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
		try {
			FileUtils.deleteDirectory(dirMetaFile(indexDir, meta));
			FileUtils.deleteDirectory(idxFile(indexDir, meta));
			return true;
		} catch (IOException e) {
		}
		return false;
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