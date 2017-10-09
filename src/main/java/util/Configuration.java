package util;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import core.commom.WritableComparable.WritableComparableFactory;
import core.commom.WritableFactory;
import core.lsmo.octree.OctreePrepareForWriteVisitor;
import core.lsmt.compact.ICompactStrategy;

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
		return new File(props.getProperty("datadir", "/tmp"), "datadir");
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
		return new File(props.getProperty("datadir", "/tmp/oplog"), "oplog");
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

	public WritableComparableFactory getDirKeyFactory() {
		// return WritableComparable.StringKeyFactory.INSTANCE;
		String valueClass = props.getProperty("dir_key_fatory", "core.commom.WritableComparable$StringKeyFactory");
		try {
			return (WritableComparableFactory) Enum
					.valueOf(Class.forName(valueClass).asSubclass(Enum.class), "INSTANCE");
		} catch (ClassNotFoundException e) {
			throw new RuntimeException(e);
		}
	}

	public WritableFactory getDirValueFactory() {
		String valueClass = props.getProperty("dir_value_fatory", "core.lsmt.WritableFactory$DirEntryFactory");
		try {
			return (WritableFactory)  Enum
					.valueOf(Class.forName(valueClass).asSubclass(Enum.class), "INSTANCE");
		} catch (ClassNotFoundException e) {
			throw new RuntimeException(e);
		}
	}

	WritableComparableFactory factory = null;

	public WritableComparableFactory getSecondaryKeyFactory() {
		// TODO : implement two factories:one for [seglistkey], one for
		// [encoding]
		// 不需要bucketID
		String valueClass = props.getProperty("secondary_key_factory", "core.lsmt.WritableComparableKey$EncodingFactory");
		try {
			if (factory == null)
				factory = (WritableComparableFactory) Enum.valueOf(Class.forName(valueClass).asSubclass(Enum.class),
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
	
	public String getIndexFactory() {
		return props.getProperty("index_factory", "core.lsmi.SortedListBasedLSMTFactory");
	}

	public float getSplitingRatio() {
		return Float.parseFloat(props.getProperty("split_ratio", "2"));
	}

	public boolean shouldCompact() {
		return Boolean.parseBoolean(props.getProperty("compact", "true"));
	}

	public int queryStartTime() {
		return Integer.parseInt(props.getProperty("starttime", "0"));
	}

	public ICompactStrategy getCompactStragety() {
		try {
			return (ICompactStrategy) Class
					.forName(props.getProperty("compactStragety", "core.lsmt.compact.LSMCompactStrategy"))
					.newInstance();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public boolean indexLeafOnly() {
		return Boolean.parseBoolean(props.getProperty("indexLeafOnly", "true"));
	}

	public boolean standaloneSentinal() {
		return Boolean.parseBoolean(props.getProperty("standaloneSentinal", "true"));
	}
}
