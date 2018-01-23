package core.concurrent;

public enum LockStrategyFactory {
	INSTANCE;
	public ILockStrategy create(LockManager manager) {
		return new SeqLockStrategy(manager);
	}
}
