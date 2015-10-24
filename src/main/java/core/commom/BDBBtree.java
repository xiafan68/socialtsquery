package core.commom;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.Comparator;
import java.util.Iterator;

import org.apache.commons.io.output.ByteArrayOutputStream;

import Util.Configuration;
import Util.Pair;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.EnvironmentMutableConfig;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.PreloadConfig;

import core.io.Bucket.BucketID;
import core.lsmt.ISSTableWriter.DirEntry;
import core.lsmt.WritableComparableKey;

public class BDBBtree {
	private Environment env;
	private Database nodeDb;
	private EnvironmentConfig myEnvConfig = new EnvironmentConfig();
	private DatabaseConfig myDbConfig = new DatabaseConfig();
	private EnvironmentMutableConfig mutableConfig = new EnvironmentMutableConfig();
	private File dir;
	private Configuration conf;

	public BDBBtree(File dir, Configuration conf) {
		this.dir = dir;
		this.conf = conf;
	}

	static enum keyComparator implements Comparator<byte[]> {
		INSTACE;

		Configuration conf;

		public void setConf(Configuration conf) {
			this.conf = conf;
		}

		@Override
		public int compare(byte[] a, byte[] a2) {
			// TODO:实现String的比较
			WritableComparableKey t = conf.getIndexKeyFactory().createIndexKey();
			WritableComparableKey t2 = conf.getIndexKeyFactory().createIndexKey();

			try {
				t.read(new DataInputStream(new ByteArrayInputStream(a)));
				t2.read(new DataInputStream(new ByteArrayInputStream(a2)));

			} catch (IOException e) {
				e.printStackTrace();
			}
			return t.compareTo(t2);
		}
	}

	static enum valueComparator implements Comparator<byte[]> {
		INSTACE;

		Configuration conf;

		public void setConf(Configuration conf) {
			this.conf = conf;
		}

		@Override
		public int compare(byte[] a, byte[] a2) {
			// 这里byte[]是[code/seglistkey]
			WritableComparableKey t = conf.getIndexValueFactory().createIndexKey();
			WritableComparableKey t2 = conf.getIndexValueFactory().createIndexKey();
			try {
				t.read(new DataInputStream(new ByteArrayInputStream(a)));
				t2.read(new DataInputStream(new ByteArrayInputStream(a2)));

			} catch (IOException e) {
				e.printStackTrace();
			}
			return t.compareTo(t2);
		}
	}

	public void open(boolean readOnly, boolean duplicatesAllowed) {
		if (!dir.exists()) {
			dir.mkdir();
		}

		// If the environment is read-only, then
		// make the databases read-only too.
		myEnvConfig.setReadOnly(readOnly);
		myDbConfig.setReadOnly(readOnly);

		// If the environment is opened for write, then we want to be
		// able to create the environment and databases if
		// they do not exist.
		myEnvConfig.setAllowCreate(!readOnly);
		myDbConfig.setAllowCreate(!readOnly);

		// Allow transactions if we are writing to the database
		/*
		 * myEnvConfig.setTransactional(!readOnly);
		 * myDbConfig.setTransactional(!readOnly);
		 */

		myEnvConfig.setTransactional(false);
		myDbConfig.setTransactional(false);

		myDbConfig.setDeferredWrite(true);

		keyComparator.INSTACE.setConf(conf);
		myDbConfig.setBtreeComparator(keyComparator.INSTACE);

		myDbConfig.setSortedDuplicates(duplicatesAllowed);
		if (duplicatesAllowed) {
			valueComparator.INSTACE.setConf(conf);
			myDbConfig.setDuplicateComparator(valueComparator.INSTACE);
		} else {

		}

		// myDbConfig.setCacheMode(CacheMode.DYNAMIC);
		mutableConfig.setCacheSize(conf.getBTreeCacheSize());

		// myEnvConfig.setSortedDuplicates(true);

		// Open the environment
		env = new Environment(dir, myEnvConfig);
		env.setMutableConfig(mutableConfig);
		// Now open, or create and open, our databases
		nodeDb = env.openDatabase(null, dir.getName(), myDbConfig);

		if (readOnly) {
			PreloadConfig preloadConfig = new PreloadConfig();
			// preloadConfig.setMaxBytes(1024*1024*128).setMaxMillisecs(1000*60);
			// preloadConfig.setMaxBytes(cacheSize *
			// 2).setMaxMillisecs(1000*60);
			nodeDb.preload(preloadConfig);
		}
	}

	public void close() {
		nodeDb.close();
		env.cleanLog();
		env.close();
	}

	/**
	 * 插入key,value对，如果用重复，直接覆盖
	 * 
	 * @param curkey
	 * @param value
	 * @param id
	 * @throws IOException
	 */

	// 将key和code插入Btree索引
	// value: [value, id]
	public void insert(WritableComparableKey curkey, DirEntry id) throws IOException {
		DatabaseEntry key = getDBEntry(curkey);

		ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
		DataOutputStream dos = new DataOutputStream(outputStream);
		id.write(dos);
		DatabaseEntry data = new DatabaseEntry();
		data.setData(outputStream.toByteArray());

		Cursor cursor = nodeDb.openCursor(null, null);
		try {
			cursor.put(key, data);
		} finally {
			cursor.close();
		}
	}

	public DirEntry get(WritableComparableKey curkey) throws IOException {
		DatabaseEntry key = getDBEntry(curkey);
		DatabaseEntry data = new DatabaseEntry();

		Cursor cursor = nodeDb.openCursor(null, null);
		DirEntry ret = null;
		try {
			OperationStatus status = cursor.getSearchKey(key, data, LockMode.DEFAULT);
			if (status == OperationStatus.SUCCESS) {
				ret = new DirEntry(conf.getIndexKeyFactory());
				ret.read(new DataInputStream(new ByteArrayInputStream(data.getData())));
			}
		} finally {
			cursor.close();
		}
		return ret;
	}

	/**
	 * insert "hello", "10", 1 "hello", "12", 12 "9","13"
	 * 
	 * @param curkey
	 * @param value
	 * @param id
	 * @throws IOException
	 */

	// 将key和code插入Btree索引
	// value: [value, id]
	public void insert(WritableComparableKey curkey, WritableComparableKey value, BucketID id) throws IOException {
		DatabaseEntry key = getDBEntry(curkey);
		DatabaseEntry data = writeEntry(value, id);

		Cursor cursor = nodeDb.openCursor(null, null);
		try {
			cursor.put(key, data);
			cursor.close();
		} finally {
			cursor.close();
		}
	}

	private Pair<WritableComparableKey, BucketID> getKeyLast(WritableComparableKey curKey,
			WritableComparableKey curCode) throws IOException {
		DatabaseEntry key = getDBEntry(curKey);
		DatabaseEntry data = getDBEntry(curCode);
		Cursor cursor = nodeDb.openCursor(null, null);
		try {
			OperationStatus status = cursor.getSearchKeyRange(key, data, LockMode.DEFAULT);
			if (status == OperationStatus.SUCCESS) {
				WritableComparableKey readKey = conf.getIndexKeyFactory().createIndexKey();
				DataInputStream dis = new DataInputStream(new ByteArrayInputStream(key.getData()));
				readKey.read(dis);
				if (readKey.compareTo(curKey) == 0) {
					status = cursor.getNextNoDup(key, data, LockMode.DEFAULT);
					if (status == OperationStatus.SUCCESS) {
						return parsePair(data);
					} else {
						cursor.getLast(key, data, LockMode.DEFAULT);
						return parsePair(data);
					}
				} else {
					return null;
				}
			} else {
				return getKeyLast(curKey, curCode);
			}
		} finally {
			cursor.close();
		}
	}

	private DatabaseEntry writeEntry(WritableComparableKey key, BucketID id) throws IOException {

		ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
		DataOutputStream dos = new DataOutputStream(outputStream);
		key.write(dos);
		id.write(dos);
		DatabaseEntry ret = new DatabaseEntry();
		ret.setData(outputStream.toByteArray());

		return ret;
	}

	private Pair<WritableComparableKey, BucketID> parsePair(DatabaseEntry data) throws IOException {
		DataInputStream dis = new DataInputStream(new ByteArrayInputStream(data.getData()));
		WritableComparableKey value = conf.getIndexValueFactory().createIndexKey();
		value.read(dis);
		BucketID ret = new BucketID();
		ret.read(dis);
		return new Pair<>(value, ret);
	}

	/**
	 * 找到第一个key相同，小于等于code的octant的offset,
	 *
	 * @param curKey
	 * @param curCode:Encoding或者SegListKey類型
	 * @return
	 * @throws IOException
	 */
	public Pair<WritableComparableKey, BucketID> floorOffset(WritableComparableKey curKey,
			WritableComparableKey curCode) throws IOException {

		DatabaseEntry key = getDBEntry(curKey);
		DatabaseEntry data = getDBEntry(curCode);

		Cursor cursor = nodeDb.openCursor(null, null);

		try {
			// 找到第一个大于等于data的
			OperationStatus status = cursor.getSearchBothRange(key, data, LockMode.DEFAULT);
			if (status == OperationStatus.SUCCESS) {
				Pair<WritableComparableKey, BucketID> ret = parsePair(data);
				if (ret.getKey().compareTo(curCode) == 0)
					return ret;
				else {
					status = cursor.getPrevDup(key, data, LockMode.DEFAULT);
					if (status == OperationStatus.SUCCESS) {
						return parsePair(data);
					} else
						return null;
				}
			} else {
				return getKeyLast(curKey, curCode);
			}
		} finally {
			cursor.close();
		}
	}

	/**
	 * 找到第一个key相同，大于等于code的octant的offset,
	 * 
	 * @param curKey
	 * @param curCode
	 * @return
	 * @throws IOException
	 */
	public Pair<WritableComparableKey, BucketID> cellOffset(WritableComparableKey curKey, WritableComparableKey curCode)
			throws IOException {
		DatabaseEntry key = getDBEntry(curKey);
		DatabaseEntry data = getDBEntry(curCode);
		Cursor cursor = nodeDb.openCursor(null, null);
		try {
			// 找到第一个大于等于data的
			OperationStatus status = cursor.getSearchBothRange(key, data, LockMode.DEFAULT);
			if (status == OperationStatus.SUCCESS) {
				DataInputStream dis = new DataInputStream(new ByteArrayInputStream(data.getData()));
				WritableComparableKey value = conf.getIndexValueFactory().createIndexKey();
				value.read(dis);
				BucketID ret = new BucketID();
				ret.read(dis);
				return new Pair<>(value, ret);
			} else {
				return null;
			}
		} finally {
			cursor.close();
		}
	}

	public BucketID getValue(WritableComparableKey curKey) throws IOException {
		Cursor cursor = nodeDb.openCursor(null, null);
		DatabaseEntry key = getDBEntry(curKey);

		DatabaseEntry data = new DatabaseEntry();
		try {
			OperationStatus status = cursor.getSearchKey(key, data, LockMode.DEFAULT);
			if (status == OperationStatus.SUCCESS) {
				BucketID ret = new BucketID();
				ret.read(new DataInputStream(new ByteArrayInputStream(data.getData())));
				return ret;
			} else {
				return null;
			}
		} finally {
			cursor.close();
		}
	}

	private DatabaseEntry getDBEntry(WritableComparableKey key) throws IOException {
		ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
		key.write(new DataOutputStream(outputStream));
		DatabaseEntry ret = new DatabaseEntry();
		ret.setData(outputStream.toByteArray());
		return ret;
	}

	class BDBKeyIterator implements Iterator<WritableComparableKey> {
		DatabaseEntry key = new DatabaseEntry();
		DatabaseEntry value = new DatabaseEntry();
		Cursor cursor = null;

		public BDBKeyIterator() {
			OperationStatus status = null;
			cursor = nodeDb.openCursor(null, null);
			status = cursor.getFirst(key, value, LockMode.DEFAULT);
			if (status != OperationStatus.SUCCESS) {
				key = null;
				value = null;
			}
		}

		@Override
		public boolean hasNext() {
			if (key == null) {
				advance();
			}
			return key != null;
		}

		@Override
		public WritableComparableKey next() {
			if (key == null) {
				advance();
			}
			WritableComparableKey ret = conf.getIndexKeyFactory().createIndexKey();
			try {
				ret.read(new DataInputStream(new ByteArrayInputStream(key.getData())));
				key = null;
			} catch (IOException e) {
				e.printStackTrace();
			}
			return ret;
		}

		private void advance() {
			if (cursor != null) {
				OperationStatus status = null;
				key = new DatabaseEntry();
				value = new DatabaseEntry();
				status = cursor.getNext(key, value, LockMode.DEFAULT);
				if (status != OperationStatus.SUCCESS) {
					key = null;
					cursor.close();
					cursor = null;
				}
			}
		}

		@Override
		public void remove() {
		}

		@Override
		public void finalize() {
			if (cursor != null)
				cursor.close();
		}
	}

	public Iterator<WritableComparableKey> keyIterator() {
		return new BDBKeyIterator();
	}
}
