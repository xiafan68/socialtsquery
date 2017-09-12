package core.commom;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
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

import core.io.Block;
import core.io.Bucket.BucketID;
import core.lsmo.common.SkipCell;
import core.lsmt.ISSTableWriter.DirEntry;
import core.lsmt.Writable;
import core.lsmt.WritableComparable;
import util.Configuration;
import util.Pair;
import util.Profile;

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
			WritableComparable t = conf.getIndexKeyFactory().createIndexKey();
			WritableComparable t2 = conf.getIndexKeyFactory().createIndexKey();

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
			WritableComparable t = conf.getIndexValueFactory().createIndexKey();
			WritableComparable t2 = conf.getIndexValueFactory().createIndexKey();
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

		myEnvConfig.setSharedCache(true);
		myEnvConfig.setCachePercent(20);
		// 当前应用中bdb其实是只读类型的，不存在删除key的情况，因此需要讲clean和merge之类的代价降到最低
		myEnvConfig.setConfigParam(EnvironmentConfig.LOG_FILE_MAX, "1000000000");// 1gb

		myEnvConfig.setTransactional(false);
		myDbConfig.setTransactional(false);

		myDbConfig.setDeferredWrite(true);

		keyComparator.INSTACE.setConf(conf);
		myDbConfig.setBtreeComparator(keyComparator.INSTACE);

		myDbConfig.setSortedDuplicates(duplicatesAllowed);

		if (duplicatesAllowed) {
			valueComparator.INSTACE.setConf(conf);
			myDbConfig.setDuplicateComparator(valueComparator.INSTACE);
		}

		// Open the environment
		env = new Environment(dir, myEnvConfig);
		env.setMutableConfig(mutableConfig);

		// Now open, or create and open, our databases
		nodeDb = env.openDatabase(null, dir.getName(), myDbConfig);
		// nodeDb.
		if (readOnly) {
			PreloadConfig preloadConfig = new PreloadConfig();
			// preloadConfig.setMaxBytes(1024*1024*128).setMaxMillisecs(1000*60);
			// preloadConfig.setMaxBytes(cacheSize *
			// 2).setMaxMillisecs(1000*60);
			nodeDb.preload(preloadConfig);
		}
	}

	public void close() {
		if (nodeDb != null) {
			nodeDb.close();
			env.cleanLog();
			env.close();
			nodeDb = null;
			env = null;
		}

	}

	public void insertBlock(WritableComparable curkey, Block block) throws IOException {
		DatabaseEntry key = getDBEntry(curkey);

		ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
		DataOutputStream dos = new DataOutputStream(outputStream);
		block.write(dos);
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
	 * 插入key,value对，如果用重复，直接覆盖
	 * 
	 * @param curkey
	 * @param value
	 * @param id
	 * @throws IOException
	 */

	// 将key和code插入Btree索引
	// value: [value, id]
	public void insert(WritableComparable curkey, DirEntry id) throws IOException {
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

	public void get(WritableComparable curkey, DirEntry entry) throws IOException {
		Profile.instance.start("readdir");
		DatabaseEntry key = getDBEntry(curkey);
		DatabaseEntry data = new DatabaseEntry();

		Cursor cursor = nodeDb.openCursor(null, null);
		try {
			OperationStatus status = cursor.getSearchKey(key, data, LockMode.DEFAULT);
			if (status == OperationStatus.SUCCESS) {
				entry.read(new DataInputStream(new ByteArrayInputStream(data.getData())));
			}
		} finally {
			cursor.close();
		}
		Profile.instance.end("readdir");
	}

	/**
	 * used to retrieve directory metadata when bdb is used as the directory
	 * 
	 * @param curkey
	 * @return
	 * @throws IOException
	 */
	public DirEntry get(WritableComparable curkey) throws IOException {
		Profile.instance.start("readdir");
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
		Profile.instance.end("readdir");
		return ret;
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
	public void insert(WritableComparable curkey, WritableComparable value, BucketID id) throws IOException {
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

	/**
	 * get the last pair whose key equals curKey
	 * 
	 * @param curKey
	 * @param curCode
	 * @return
	 * @throws IOException
	 */
	private Pair<WritableComparable, DataInputStream> getKeyLast(WritableComparable curKey, WritableComparable curCode)
			throws IOException {
		DatabaseEntry key = getDBEntry(curKey);
		DatabaseEntry data = getDBEntry(curCode);
		Cursor cursor = nodeDb.openCursor(null, null);
		try {
			OperationStatus status = cursor.getSearchKeyRange(key, data, LockMode.DEFAULT);
			if (status == OperationStatus.SUCCESS) {
				WritableComparable readKey = conf.getIndexKeyFactory().createIndexKey();
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

	private DatabaseEntry writeEntry(WritableComparable key, Writable id) throws IOException {
		ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
		DataOutputStream dos = new DataOutputStream(outputStream);
		key.write(dos);
		id.write(dos);
		DatabaseEntry ret = new DatabaseEntry();
		ret.setData(outputStream.toByteArray());

		return ret;
	}

	private Pair<WritableComparable, DataInputStream> parsePair(DatabaseEntry data) throws IOException {
		DataInputStream dis = new DataInputStream(new ByteArrayInputStream(data.getData()));
		WritableComparable value = conf.getIndexValueFactory().createIndexKey();
		value.read(dis);
		return new Pair<>(value, dis);
	}

	/**
	 * 找到第一个key相同，小于等于code的octant的offset,
	 *
	 * @param curKey
	 * @param curCode:Encoding或者SegListKey類型
	 * @return
	 * @throws IOException
	 */
	public BucketID floorOffset(WritableComparable curKey, WritableComparable curCode) throws IOException {
		Pair<WritableComparable, DataInputStream> ret = floor(curKey, curCode);
		if (ret != null) {
			BucketID id = new BucketID();
			id.read(ret.getValue());
			return id;
		} else {
			return null;
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
	public Pair<WritableComparable, DataInputStream> floor(WritableComparable curKey, WritableComparable curCode)
			throws IOException {

		DatabaseEntry key = getDBEntry(curKey);
		DatabaseEntry data = getDBEntry(curCode);

		Cursor cursor = nodeDb.openCursor(null, null);

		try {
			// 找到第一个大于等于data的
			OperationStatus status = cursor.getSearchBothRange(key, data, LockMode.DEFAULT);
			if (status == OperationStatus.SUCCESS) {
				Pair<WritableComparable, DataInputStream> ret = parsePair(data);
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
	public BucketID cellOffset(WritableComparable curKey, WritableComparable curCode) throws IOException {
		Pair<WritableComparable, DataInputStream> ret = cell(curKey, curCode);
		if (ret == null) {
			return null;
		} else {
			BucketID bucketId = new BucketID();
			bucketId.read(ret.getValue());
			return bucketId;
		}
	}

	/**
	 * 找到第一个key相同，大于等于code的skip list的bucket,
	 * 
	 * @param curKey
	 * @param curCode
	 * @return
	 * @throws IOException
	 */
	public SkipCell cellSkipCell(WritableComparable curKey, WritableComparable curCode) throws IOException {
		Pair<WritableComparable, DataInputStream> ret = cell(curKey, curCode);
		if (ret == null) {
			return null;
		} else {
			SkipCell cell = new SkipCell(-1, conf.getIndexValueFactory());
			cell.read(ret.getValue());
			return cell;
		}
	}
	
	
	/**
	 * 找到第一个key相同，大于等于code的skip list的bucket,
	 * 
	 * @param curKey
	 * @param curCode
	 * @return
	 * @throws IOException
	 */
	public SkipCell floorSkipCell(WritableComparable curKey, WritableComparable curCode) throws IOException {
		Pair<WritableComparable, DataInputStream> ret = floor(curKey, curCode);
		if (ret == null) {
			return null;
		} else {
			SkipCell cell = new SkipCell(-1, conf.getIndexValueFactory());
			cell.read(ret.getValue());
			return cell;
		}
	}

	/**
	 * insert a SkipCell
	 * 
	 * @param key
	 * @param cell
	 * @throws IOException
	 */
	public void insertSkipCell(WritableComparable key, SkipCell cell) throws IOException {
		DatabaseEntry dbKey = getDBEntry(key);
		DatabaseEntry value = writeEntry(cell.getIndexEntry(0).getKey(), cell);

		Cursor cursor = nodeDb.openCursor(null, null);
		try {
			cursor.put(dbKey, value);
			cursor.close();
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
	public Pair<WritableComparable, DataInputStream> cell(WritableComparable curKey, WritableComparable curCode)
			throws IOException {
		DatabaseEntry key = getDBEntry(curKey);
		DatabaseEntry data = getDBEntry(curCode);
		Cursor cursor = nodeDb.openCursor(null, null);
		try {
			// 找到第一个大于等于data的
			OperationStatus status = cursor.getSearchBothRange(key, data, LockMode.DEFAULT);
			if (status == OperationStatus.SUCCESS) {
				DataInputStream dis = new DataInputStream(new ByteArrayInputStream(data.getData()));
				WritableComparable value = conf.getIndexValueFactory().createIndexKey();
				value.read(dis);
				return new Pair<>(value, dis);
			} else {
				return null;
			}
		} finally {
			cursor.close();
		}
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
			WritableComparable ret = conf.getIndexKeyFactory().createIndexKey();
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
