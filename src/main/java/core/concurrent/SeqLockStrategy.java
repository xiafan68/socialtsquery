package core.concurrent;

import java.util.List;

/**
 * 本加锁策略只有在读取或者写某个词的倒排列表时才加锁
 * 
 * @author xiafan
 *
 */
public class SeqLockStrategy implements ILockStrategy {
	LockManager manager;

	public SeqLockStrategy(LockManager manager) {
		this.manager = manager;
	}

	@Override
	public void readPrepare(List<String> keywords) {

	}

	@Override
	public void writePrepare(List<String> keywords) {
	}

	@Override
	public void read(String keyword) {
		manager.postReadLock(keyword.hashCode());
	}

	@Override
	public void readOver(String keyword) {
		manager.postReadUnLock(keyword.hashCode());
	}

	@Override
	public void write(String keyword) {
		manager.postWriteLock(keyword.hashCode());
	}

	@Override
	public void writeOver(String keyword) {
		manager.postWriteUnLock(keyword.hashCode());
	}

	@Override
	public void cleanup() {
	}
}
