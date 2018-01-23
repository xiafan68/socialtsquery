package core.concurrent;

import org.junit.Test;

public class LockManagerTest {
	@Test
	public void test() {
		LockManager manager = new LockManager();
		// 测试lock是否可以重复加锁
		manager.postReadLock(1);
		manager.postReadLock(1);
		manager.postReadUnLock(1);
		manager.postReadUnLock(1);
	}
}
