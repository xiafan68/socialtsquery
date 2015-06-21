package core.index;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import common.MidSegment;

/**
 * 
 * @author xiafan
 *
 */
public class LSMOInvertedIndex {
	private Map<String, LogStructureOctree> dir;

	AtomicBoolean running = new AtomicBoolean(true);
	File dataDir;

	// locks for posting list
	ReadWriteLock[] locks;
	// locks for flushing
	ReadWriteLock flushLock;
	CommitLog opLog;
	Map<String, LogStructureOctree> memtable;
	AtomicInteger totalSegs = new AtomicInteger(0);
	int maxVersion;

	FlushService flushService;

	public LSMOInvertedIndex() {

	}

	public void init() {
		// critical to the parallelism
		locks = new ReadWriteLock[1024];
		flushLock = new ReentrantReadWriteLock();
		for (int i = 0; i < 1024; i++) {
			locks[i] = new ReentrantReadWriteLock();
		}
		flushService = new FlushService(this);
		flushService.start();

	}

	/**
	 * get the lock for word
	 * @param word
	 * @return
	 */
	public ReadWriteLock getLock(String word) {
		return locks[Math.abs(word.hashCode()) % locks.length];
	}

	public LogStructureOctree getPostingList(String word) {
		LogStructureOctree ret = dir.get(word);
		if (!ret.increRef()) {
			ret = null;
		}
		return ret;
	}

	public void insert(List<String> keywords, MidSegment seg) {
		flushLock.readLock().lock();
		try {
			for (String keyword : keywords) {
				ReadWriteLock lock = getLock(keyword);
				lock.writeLock().lock();
				try {
					LogStructureOctree postinglist;
					if (memtable.containsKey(keyword)) {
						postinglist = memtable.get(keyword);
					} else {
						postinglist = new LogStructureOctree();
						memtable.put(keyword, postinglist);
					}
					postinglist.insert(seg);
				} finally {
					lock.writeLock().unlock();
				}
			}
			maySwitchMemtable();
		} finally {
			flushLock.readLock().unlock();
		}
	}

	public void insert(String keywords, MidSegment seg) {

	}

	// TODO check whether we need to switch the memtable
	private void maySwitchMemtable() {
		boolean violated = false;
		if (violated) {
			flushLock.writeLock().lock();
			try {
				// check violation again
				// open new log
				opLog.openNewLog(maxVersion++);
				flushMemtable();
			} finally {
				flushLock.writeLock().unlock();
			}
		}
	}

	/**
	 * 写出当前memtable
	 */
	public void flushMemtable() {
		int curV = maxVersion++;
		for (LogStructureOctree posting : memtable.values()) {
			posting.flushMemOctree(curV);
		}
	}

	public int getCompactNum() {
		return 1;
	}

	public int getFlushNum() {
		// TODO Auto-generated method stub
		return 1;
	}
}
