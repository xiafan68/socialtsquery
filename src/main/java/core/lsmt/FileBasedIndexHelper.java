package core.lsmt;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

import Util.Configuration;
import core.io.Bucket.BucketID;
import core.lsmt.IMemTable.SSTableMeta;

public class FileBasedIndexHelper extends IndexHelper {

	protected FileOutputStream indexFileDos;
	protected DataOutputStream indexDos; // write down the encoding of each
											// block
	private BufferedOutputStream indexBuffer;
	protected DataOutputStream dirDos; // write directory meta

	private BufferedOutputStream dirBuffer;
	private FileOutputStream dirFileOs;

	public FileBasedIndexHelper(Configuration conf) {
		super(conf);
	}

	public void openIndexFile(File dir, SSTableMeta meta)
			throws FileNotFoundException {
		if (!dir.exists())
			dir.mkdirs();

		indexFileDos = new FileOutputStream(idxFile(dir, meta));
		indexBuffer = new BufferedOutputStream(indexFileDos);
		indexDos = new DataOutputStream(indexBuffer);

		dirFileOs = new FileOutputStream(dirMetaFile(dir, meta));
		dirBuffer = new BufferedOutputStream(dirFileOs);
		dirDos = new DataOutputStream(dirBuffer);
	}

	@Override
	public void startPostingList(WritableComparableKey key,
			BucketID newListStart) throws IOException {
		super.startPostingList(key, newListStart);
		if (indexBuffer != null)
			indexBuffer.flush();
		curDir.indexStartOffset = indexFileDos.getChannel().position();
	}

	@Override
	public void endPostingList(BucketID postingListEnd) throws IOException {
		super.endPostingList(postingListEnd);
		curDir.write(dirDos);
	}

	@Override
	public void moveToDir(File preDir, File dir, SSTableMeta meta) {
		File tmpFile = idxFile(preDir, meta);
		tmpFile.renameTo(idxFile(dir, meta));
		tmpFile = dirMetaFile(preDir, meta);
		tmpFile.renameTo(dirMetaFile(dir, meta));
	}

	public void buildIndex(WritableComparableKey code, BucketID id)
			throws IOException {
		curDir.sampleNum++;
		code.write(indexDos);
		id.write(indexDos);
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
		if (indexFileDos != null) {
			indexFileDos.close();
			indexFileDos = null;
			indexDos.close();
			indexBuffer.close();

			dirFileOs.close();
			dirDos.close();
			dirBuffer.close();
		}
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

	@Override
	public boolean delete(File indexDir, SSTableMeta meta) {
		boolean ret = dirMetaFile(indexDir, meta).delete();
		ret = ret && idxFile(indexDir, meta).delete();
		return ret;
	}
}
