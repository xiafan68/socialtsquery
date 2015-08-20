package core.lsmt;

import java.io.File;
import java.io.IOException;
import java.util.Comparator;

import core.io.Bucket;
import core.lsmt.IMemTable.SSTableMeta;

public interface ISSTableWriter {

	public SSTableMeta getMeta();

	public void write(File dir) throws IOException;

	public void close() throws IOException;

	public static class IntegerComparator implements Comparator<Integer> {
		public static IntegerComparator instance = new IntegerComparator();

		@Override
		public int compare(Integer o1, Integer o2) {
			return Integer.compare(o1, o2);
		}
	}

	/**
	 * return a bucket
	 * @return
	 */
	public Bucket getBucket();

	/**
	 * create a new bucket
	 * @return
	 */
	public Bucket newBucket();
}
