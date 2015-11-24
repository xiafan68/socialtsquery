package util;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import core.lsmo.octree.OctreePrepareForWriteVisitor;
import core.lsmt.WritableComparableKey;
import core.lsmt.WritableComparableKey.WritableComparableKeyFactory;

public class Configuration {
	Properties props = new Properties();

	public void load(String path) throws IOException {
		props.load(new FileInputStream(path));
		OctreePrepareForWriteVisitor.INSTANCE.splitingRatio = getSplitingRatio();
		OctreePrepareForWriteVisitor.INSTANCE.octantLimit = getOctantSizeLimit();
	}

	/**
	 * this value specifies the sampling gap of the encoding of octants
	 *
	 * @return
	 */
	public int getIndexStep() {
		String step = props.getProperty("index_step", "128");
		return Integer.parseInt(step);
	}

	/**
	 * the directory
	 *
	 * @return
	 */
	public File getIndexDir() {
		return new File(props.getProperty("indexdir", "/tmp"));
	}

	/**
	 * the directory
	 *
	 * @return
	 */
	public File getTmpDir() {
		return new File(props.getProperty("tmpdir", "/tmp"));
	}

	public File getCommitLogDir() {
		return new File(props.getProperty("oplogdir", "/tmp/oplog"));
	}

	public int getFlushLimit() {
		return Integer.parseInt(props.getProperty("memtable_size_limit", "2000000"));
	}

	public boolean debugMode() {
		return Boolean.parseBoolean(props.getProperty("debug", "false"));
	}

	/**
	 * determine the max number of blocks an octant may occupy
	 *
	 * @return
	 */
	public int getOctantSizeLimit() {
		return Integer.parseInt(props.getProperty("octant_size_limit", "350"));
	}

	public int getBatchCommitNum() {
		return Integer.parseInt(props.getProperty("batch_commit_num", "1000"));
	}

	public WritableComparableKeyFactory getIndexKeyFactory() {
		return WritableComparableKey.StringKeyFactory.INSTANCE;
	}

	WritableComparableKeyFactory factory = null;

	public WritableComparableKeyFactory getIndexValueFactory() {
		// TODO : implement two factories:one for [seglistkey], one for
		// [encoding]
		// 不需要bucketID
		String valueClass = props.getProperty("value_factory", "core.lsmt.WritableComparableKey$EncodingFactory");
		try {
			if (factory == null)
				factory = (WritableComparableKeyFactory) Enum.valueOf(Class.forName(valueClass).asSubclass(Enum.class),
						"INSTANCE");

			return factory;
		} catch (ClassNotFoundException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public String toString() {
		return "Configuration [props=" + props + "]";
	}

	public long getDurationTime() {
		return Long.parseLong(props.getProperty("duration_time", "3600000"));
	}

	public long getBTreeCacheSize() {
		// TODO Auto-generated method stub
		return 0;
	}

	public String getIndexHelper() {
		return props.getProperty("indexhelper", "core.lsmt.bdbindex.BDBBasedIndexHelper");
	}

	public String getIndexFactory() {
		return props.getProperty("index_factory", "core.lsmi.SortedListBasedLSMTFactory");
	}

	public float getSplitingRatio() {
		return Float.parseFloat(props.getProperty("split_ratio", "2"));
	}

	public boolean shouldCompact() {
		return Boolean.parseBoolean(props.getProperty("compact", "true"));
	}
}
