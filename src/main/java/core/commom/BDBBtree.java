package core.commom;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
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
		myDbConfig.setBtreeComparator(btreeComparatorClass);
		if (duplicatesAllowed)
			myDbConfig.setDuplicateComparator(NodeComparator.class);
		// myDbConfig.setCacheMode(CacheMode.DYNAMIC);
		mutableConfig.setCacheSize(conf.getBTreeCacheSize());

		// myEnvConfig.setSortedDuplicates(true);
		myDbConfig.setSortedDuplicates(duplicatesAllowed);

		// Open the environment
		env = new Environment(dir, myEnvConfig);
		env.setMutableConfig(mutableConfig);
		// Now open, or create and open, our databases
		nodeDb = env.openDatabase(null, "NodesDB", myDbConfig);

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

	private Pair<WritableComparableKey, BucketID> getKeyLast(
			WritableComparableKey curKey, WritableComparableKey curCode)
			throws IOException {
		DatabaseEntry key = getDBEntry(curKey);
		DatabaseEntry data = getDBEntry(curCode);
		Cursor cursor = nodeDb.openCursor(null, null);
		try {
			OperationStatus status = cursor.getSearchKeyRange(key, data,
					LockMode.DEFAULT);
			if (status == OperationStatus.SUCCESS) {
				WritableComparableKey readKey = conf.getMemTableKey()
						.createIndexKey();
				DataInputStream dis = new DataInputStream(
						new ByteArrayInputStream(data.getData()));
				readKey.read(dis);
				if (readKey.compareTo(curKey) == 0) {
					status = cursor.getNextNoDup(key, data, LockMode.DEFAULT);
					if (status == OperationStatus.SUCCESS) {
						return parsePair(data);
					} else {
						return null;
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

	private Pair<WritableComparableKey, BucketID> parsePair(DatabaseEntry data)
			throws IOException {
		DataInputStream dis = new DataInputStream(new ByteArrayInputStream(
				data.getData()));
		WritableComparableKey value = conf.getIndexValueFactory()
				.createIndexKey();
		value.read(dis);
		BucketID ret = new BucketID();
		ret.read(dis);
		return new Pair<>(value, ret);
	}

	public Pair<WritableComparableKey, BucketID> cellOffset(
			WritableComparableKey curKey, WritableComparableKey curCode)
			throws IOException {
		DatabaseEntry key = getDBEntry(curKey);
		DatabaseEntry data = getDBEntry(curCode);
		Cursor cursor = nodeDb.openCursor(null, null);
		try {
			OperationStatus status = cursor.getSearchBothRange(key, data,
					LockMode.DEFAULT);
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

	public Pair<WritableComparableKey, BucketID> floorOffset(
			WritableComparableKey curKey, WritableComparableKey curCode)
			throws IOException {
		DatabaseEntry key = getDBEntry(curKey);
		DatabaseEntry data = getDBEntry(curCode);
		Cursor cursor = nodeDb.openCursor(null, null);
		try {
			OperationStatus status = cursor.getSearchBothRange(key, data,
					LockMode.DEFAULT);
			if (status == OperationStatus.SUCCESS) {
				DataInputStream dis = new DataInputStream(
						new ByteArrayInputStream(data.getData()));
				WritableComparableKey value = conf.getIndexValueFactory()
						.createIndexKey();
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
			OperationStatus status = cursor.getSearchKey(key, data,
					LockMode.DEFAULT);
			if (status == OperationStatus.SUCCESS) {
				BucketID ret = new BucketID();
				ret.read(new DataInputStream(new ByteArrayInputStream(data
						.getData())));
				return ret;
			} else {
				return null;
			}
		} finally {
			cursor.close();
		}
	}

	private DatabaseEntry getDBEntry(WritableComparableKey key)
			throws IOException {
		ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
		key.write(new DataOutputStream(outputStream));
		DatabaseEntry ret = new DatabaseEntry();
		ret.setData(outputStream.toByteArray());
		return ret;
	}

	public Iterator<WritableComparableKey> keyIterator() {
		return new Iterator<WritableComparableKey>() {
			DatabaseEntry key;
			DatabaseEntry value;

			@Override
			public boolean hasNext() {
				// TODO Auto-generated method stub
				return false;
			}

			@Override
			public WritableComparableKey next() {
				// TODO Auto-generated method stub
				return null;
			}

			@Override
			public void remove() {
				// TODO Auto-generated method stub

			}
		};
	}
}
