package core.commom;

import java.io.File;

import core.lsmt.IMemTable.SSTableMeta;

public class IndexFileUtils {
	public static File idxFile(File dir, SSTableMeta meta) {
		return new File(dir, String.format("%d_%d_idx", meta.version, meta.level));
	}

	public static File dirMetaFile(File dir, SSTableMeta meta) {
		return new File(dir, String.format("%d_%d_dir", meta.version, meta.level));
	}

	public static File dataFile(File dir, SSTableMeta meta) {
		return new File(dir, String.format("%d_%d.data", meta.version, meta.level));
	}

	public static File markFile(File dir, SSTableMeta meta) {
		return new File(dir, String.format("%d_%d.mark", meta.version, meta.level));
	}
}
