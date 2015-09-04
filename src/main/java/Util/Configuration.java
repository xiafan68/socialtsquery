package Util;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import core.lsmt.WritableComparableKey;
import core.lsmt.WritableComparableKey.WritableComparableKeyFactory;

public class Configuration {
	Properties props = new Properties();

	public void load(String path) throws IOException {
		props.load(new FileInputStream(path));
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
		return Integer.parseInt(props.getProperty("memtable_size_limit",
				"2000000"));
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
		return Integer.parseInt(props.getProperty("octant_size_limit", "120"));
	}

	public int getBatchCommitNum() {
		return Integer.parseInt(props.getProperty("batch_commit_num", "1000"));
	}

	public WritableComparableKeyFactory getMemTableKey() {
		return WritableComparableKey.StringKeyFactory.INSTANCE;
	}

	@Override
	public String toString() {
		return "Configuration [props=" + props + "]";
	}

	public long getDurationTime() {
		return Long.parseLong(props.getProperty("duration_time", "3600000"));
	}

}
