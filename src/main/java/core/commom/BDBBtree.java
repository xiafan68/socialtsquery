package core.commom;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.Comparator;
import java.util.Iterator;

import org.apache.commons.io.output.ByteArrayOutputStream;

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

import core.commom.WritableComparable.WritableComparableFactory;
import util.Pair;

/**
 * class for reading and writing keys and (duplicated) values into Berkeley DB
 * 
 * @author xiafan
 *
 */
public class BDBBtree {
	private Environment env;
	private Database nodeDb;
	private EnvironmentConfig myEnvConfig = new EnvironmentConfig();
	private DatabaseConfig myDbConfig = new DatabaseConfig();
	private EnvironmentMutableConfig mutableConfig = new EnvironmentMutableConfig();
	private BDBBTreeBuilder bdbbTreeBuilder;

	public BDBBtree(BDBBTreeBuilder bdbbTreeBuilder) {
		this.bdbbTreeBuilder = bdbbTreeBuilder;
	}

	private static class BDBComparator implements Comparator<byte[]>, Serializable {
		WritableComparableFactory factory;

		public BDBComparator() {
		}

		public BDBComparator(WritableComparableFactory factory) {
			this.factory = factory;
		}

		@Override
		public int compare(byte[] a, byte[] a2) {
			WritableComparable t = factory.create();
			WritableComparable t2 = factory.create();

			try {
				t.read(new DataInputStream(new ByteArrayInputStream(a)));
				t2.read(new DataInputStream(new ByteArrayInputStream(a2)));

			} catch (IOException e) {
				e.printStackTrace();
			}
			return t.compareTo(t2);
		}
	}

	public void open() {
		if (!bdbbTreeBuilder.getDir().exists()) {
			bdbbTreeBuilder.getDir().mkdir();
		}

		// If the environment is read-only, then
		// make the databases read-only too.
		myEnvConfig.setReadOnly(bdbbTreeBuilder.isReadOnly());
		myDbConfig.setReadOnly(bdbbTreeBuilder.isReadOnly());

		// If the environment is opened for write, then we want to be
		// able to create the environment and databases if
		// they do not exist.
		myEnvConfig.setAllowCreate(!bdbbTreeBuilder.isReadOnly());
		myDbConfig.setAllowCreate(!bdbbTreeBuilder.isReadOnly());

		myEnvConfig.setSharedCache(true);
		myEnvConfig.setCachePercent(20);
		// 当前应用中bdb其实是只读类型的，不存在删除key的情况，因此需要讲clean和merge之类的代价降到最低
		myEnvConfig.setConfigParam(EnvironmentConfig.LOG_FILE_MAX, "1000000000");// 1gb

		myEnvConfig.setTransactional(false);
		myDbConfig.setTransactional(false);

		myDbConfig.setDeferredWrite(true);

		myDbConfig.setBtreeComparator(new BDBComparator(bdbbTreeBuilder.getKeyFactory()));

		myDbConfig.setSortedDuplicates(bdbbTreeBuilder.isAllowDuplicates());

		if (bdbbTreeBuilder.isAllowDuplicates()) {
			myDbConfig.setDuplicateComparator(new BDBComparator(bdbbTreeBuilder.getSecondaryKeyFactory()));
		}

		// Open the environment
		env = new Environment(bdbbTreeBuilder.getDir(), myEnvConfig);
		env.setMutableConfig(mutableConfig);

		// Now open, or create and open, our databases
		nodeDb = env.openDatabase(null, bdbbTreeBuilder.getDir().getName(), myDbConfig);
		// nodeDb.
		if (bdbbTreeBuilder.isReadOnly()) {
			PreloadConfig preloadConfig = new PreloadConfig();
			nodeDb.preload(preloadConfig);
		}
	}

	public void close() {
		if (nodeDb != null) {
			nodeDb.close();
			if (!myEnvConfig.getReadOnly())
				env.cleanLog();
			env.close();
			nodeDb = null;
			env = null;
		}
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
	public void insert(WritableComparable curkey, Writable value) throws IOException {
		DatabaseEntry key = getDBEntry(curkey);

		ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
		DataOutputStream dos = new DataOutputStream(outputStream);
		value.write(dos);
		DatabaseEntry data = new DatabaseEntry();
		data.setData(outputStream.toByteArray());

		Cursor cursor = nodeDb.openCursor(null, null);
		try {
			cursor.put(key, data);
		} finally {
			cursor.close();
		}
	}

	/**
	 * the key of bdb index is curkey, the value is the combination of value and
	 * id
	 * 
	 * @param curkey
	 * @param value
	 * @param id
	 * @throws IOException
	 */

	// 将key和code插入Btree索引
	// value: [value, id]
	public void insert(WritableComparable curkey, WritableComparable secondaryKey, Writable value) throws IOException {
		DatabaseEntry key = getDBEntry(curkey);
		DatabaseEntry data = writeEntry(secondaryKey, value);

		Cursor cursor = nodeDb.openCursor(null, null);
		try {
			cursor.put(key, data);
			cursor.close();
		} finally {
			cursor.close();
		}
	}

	public Writable get(WritableComparable curKey) throws IOException {
		DatabaseEntry key = getDBEntry(curKey);
		DatabaseEntry data = new DatabaseEntry();

		Cursor cursor = nodeDb.openCursor(null, null);
		Writable ret = null;
		try {
			OperationStatus status = cursor.getSearchKey(key, data, LockMode.DEFAULT);
			if (status == OperationStatus.SUCCESS) {
				ret = bdbbTreeBuilder.getValueFactory().create();
				ret.read(new DataInputStream(new ByteArrayInputStream(data.getData())));
			}
		} finally {
			cursor.close();
		}

		return ret;
	}

	public Pair<WritableComparable, Writable> cell(WritableComparable curKey, WritableComparable secondaryKey)
			throws IOException {
		DatabaseEntry key = getDBEntry(curKey);
		DatabaseEntry data = getDBEntry(secondaryKey);
		Cursor cursor = nodeDb.openCursor(null, null);

		try {
			// 找到第一个大于等于data的
			OperationStatus status = cursor.getSearchBothRange(key, data, LockMode.DEFAULT);
			if (status == OperationStatus.SUCCESS) {
				return parsePair(data);
			} else {
				return null;
			}
		} finally {
			cursor.close();
		}
	}

	/**
	 * 找到第一个key相同，小于等于code的octant的offset,
	 *
	 * @param curKey
	 * @param curCode:Encoding或者SegListKey類型
	 * @return
	 * @throws IOException
	 */
	public Pair<WritableComparable, Writable> floor(WritableComparable curKey, WritableComparable secondaryKey)
			throws IOException {

		DatabaseEntry key = getDBEntry(curKey);
		DatabaseEntry data = getDBEntry(secondaryKey);

		Cursor cursor = nodeDb.openCursor(null, null);

		try {
			// 找到第一个大于等于data的
			OperationStatus status = cursor.getSearchBothRange(key, data, LockMode.DEFAULT);
			if (status == OperationStatus.SUCCESS) {
				Pair<WritableComparable, Writable> ret = parsePair(data);
				if (ret.getKey().compareTo(secondaryKey) == 0)
					return ret;
				else {
					status = cursor.getPrevDup(key, data, LockMode.DEFAULT);
					if (status == OperationStatus.SUCCESS) {
						return parsePair(data);
					} else
						return null;
				}
			} else {
				return getKeyLast(curKey, secondaryKey);
			}
		} finally {
			cursor.close();
		}
	}

	/**
	 * get the last pair whose key equals curKey
	 * 
	 * @param curKey
	 * @param curCode
	 * @return
	 * @throws IOException
	 */
	private Pair<WritableComparable, Writable> getKeyLast(WritableComparable curKey, WritableComparable curCode)
			throws IOException {
		DatabaseEntry key = getDBEntry(curKey);
		DatabaseEntry data = getDBEntry(curCode);
		Cursor cursor = nodeDb.openCursor(null, null);
		try {
			OperationStatus status = cursor.getSearchKeyRange(key, data, LockMode.DEFAULT);
			if (status == OperationStatus.SUCCESS) {
				WritableComparable readKey = bdbbTreeBuilder.getKeyFactory().create();
				DataInputStream dis = new DataInputStream(new ByteArrayInputStream(key.getData()));
				readKey.read(dis);
				if (readKey.compareTo(curKey) == 0) {
					status = cursor.getNextNoDup(key, data, LockMode.DEFAULT);
					if (status == OperationStatus.SUCCESS) {
						cursor.getPrev(key, data, LockMode.DEFAULT);
						return parsePair(data);
					} else {
						cursor.getLast(key, data, LockMode.DEFAULT);
						return parsePair(data);
					}
				} else {
					return null;
				}
			} else {
				return null;
			}
		} finally {
			cursor.close();
		}
	}

	private DatabaseEntry writeEntry(WritableComparable key, Writable value) throws IOException {
		ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
		DataOutputStream dos = new DataOutputStream(outputStream);
		key.write(dos);
		value.write(dos);
		DatabaseEntry ret = new DatabaseEntry();
		ret.setData(outputStream.toByteArray());

		return ret;
	}

	private Pair<WritableComparable, Writable> parsePair(DatabaseEntry data) throws IOException {
		DataInputStream dis = new DataInputStream(new ByteArrayInputStream(data.getData()));
		WritableComparable curSecondaryKey = bdbbTreeBuilder.getSecondaryKeyFactory().create();
		curSecondaryKey.read(dis);
		Writable value = bdbbTreeBuilder.getValueFactory().create();
		value.read(dis);

		return new Pair<>(curSecondaryKey, value);
	}

	private DatabaseEntry getDBEntry(WritableComparable key) throws IOException {
		ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
		key.write(new DataOutputStream(outputStream));
		DatabaseEntry ret = new DatabaseEntry();
		ret.setData(outputStream.toByteArray());
		return ret;
	}

	public class BDBKeyIterator implements Iterator<WritableComparable> {
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
		public WritableComparable next() {
			if (key == null) {
				advance();
			}
			WritableComparable ret = bdbbTreeBuilder.getKeyFactory().create();
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

		public void close() {
			if (cursor != null) {
				cursor.close();
			}
		}

		@Override
		public void remove() {
		}

		@Override
		public void finalize() {
			if (cursor != null) {
				cursor.close();
				cursor = null;
			}
		}
	}

	public Iterator<WritableComparable> keyIterator() {
		return new BDBKeyIterator();
	}
}
