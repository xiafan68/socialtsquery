package core.lsmt;

import java.io.File;
import java.io.IOException;

import core.io.Bucket.BucketID;
import core.lsmt.IMemTable.SSTableMeta;
import core.lsmt.ISSTableWriter.DirEntry;
import util.Configuration;

/**
 * 用于构建数据文件对应的索引文件
 * 
 * @author xiafan
 *
 */
public abstract class IndexHelper {
	protected DirEntry curDir;
	protected Configuration conf;

	public IndexHelper(Configuration conf) {
		curDir = new DirEntry(conf.getIndexKeyFactory());
		this.conf = conf;
	}

	public void setupDataStartBlockIdx(BucketID newListStart) {
		curDir.startBucketID.copy(newListStart);
	}

	public void startPostingList(WritableComparableKey key, BucketID newListStart) throws IOException {
		if (newListStart != null)
			curDir.startBucketID.copy(newListStart);
		curDir.curKey = key;
		curDir.sampleNum = 0;
		curDir.size = 0;
		curDir.minTime = Integer.MAX_VALUE;
		curDir.maxTime = Integer.MIN_VALUE;
	}

	public void endPostingList(BucketID postingListEnd) throws IOException {
		curDir.endBucketID.copy(postingListEnd);
	}

	public void process(int startTime, int endTime) {
		curDir.size++;
		curDir.minTime = Math.min(curDir.minTime, startTime);
		curDir.maxTime = Math.max(curDir.maxTime, endTime);
	}

	public abstract void openIndexFile(File dir, SSTableMeta meta) throws IOException;

	public abstract void buildIndex(WritableComparableKey code, BucketID id) throws IOException;

	public DirEntry getDirEntry() {
		return curDir;
	}

	public abstract void moveToDir(File preDir, File dir, SSTableMeta meta);

	public abstract void close() throws IOException;

	public abstract boolean validate(SSTableMeta meta);

	public abstract boolean delete(File indexDir, SSTableMeta meta);

	@Override
	public void finalize() {
		try {
			close();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
}
