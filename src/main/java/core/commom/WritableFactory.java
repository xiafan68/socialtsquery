package core.commom;

import core.lsmo.MarkDirEntry;
import core.lsmt.DirEntry;

public interface WritableFactory {
	public Writable create();

	public static class DirEntryFactory implements WritableFactory {

		@Override
		public Writable create() {
			return new DirEntry();
		}
	}

	public static class MarkDirEntryFactory implements WritableFactory {

		@Override
		public Writable create() {
			return new MarkDirEntry();
		}
	}
}
