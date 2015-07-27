package core.index;

import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public enum LockManager {
	INSTANCE;

	// locks for posting list
	private ReadWriteLock[] postsLocks;
	// any operation to the version set needs to occupy this lock
	private ReadWriteLock versionLock;
	// public static LockManager instance = new LockManager();
	private boolean bootstrap = false;

	private LockManager() {
		postsLocks = new ReadWriteLock[1024];
		versionLock = new ReentrantReadWriteLock();
		for (int i = 0; i < 1024; i++) {
			postsLocks[i] = new ReentrantReadWriteLock();
		}
	}

	public void setBootStrap() {
		bootstrap ^= bootstrap;
	}

	public void postReadLock(int keyword) {
		postsLocks[keyword % postsLocks.length].readLock().lock();
	}

	public void postReadUnLock(int keyword) {
		postsLocks[keyword % postsLocks.length].readLock().unlock();
	}

	public void postWriteLock(int keyword) {
		postsLocks[keyword % postsLocks.length].writeLock().lock();
	}

	public void postWriteUnLock(int keyword) {
		postsLocks[keyword % postsLocks.length].writeLock().unlock();
	}

	public void versionReadLock() {
		versionLock.readLock().lock();
	}

	public void versionReadUnLock() {
		versionLock.readLock().unlock();
	}

	public void versionWriteLock() {
		versionLock.writeLock().lock();
	}

	public void versionWriteUnLock() {
		versionLock.writeLock().unlock();
	}

	public void shutdown() {
		versionLock = null;
		for (int i = 0; i < postsLocks.length; i++)
			postsLocks[i] = null;

	}
}
