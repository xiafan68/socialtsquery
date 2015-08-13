package Util;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

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
		return Boolean.parseBoolean(props.getProperty("debug", "true"));
	}

	/**
	 * determine the max number of blocks an octant may occupy
	 * @return
	 */
	public int getOctantSizeLimit() {
		return Integer.parseInt(props.getProperty("octant_size_limit", "120"));
	}
}
