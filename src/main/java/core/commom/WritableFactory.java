package core.commom;

import core.commom.WritableComparableKey.EncodingFactory;
import core.lsmo.MarkDirEntry;
import core.lsmo.persistence.SkipCell;
import core.lsmt.DirEntry;

public interface WritableFactory {
	public Writable create();

	public static enum DirEntryFactory implements WritableFactory {
		INSTANCE;
		@Override
		public Writable create() {
			return new DirEntry();
		}
	}

	public static enum MarkDirEntryFactory implements WritableFactory {
		INSTANCE;
		@Override
		public Writable create() {
			return new MarkDirEntry();
		}
	}

	public static enum SkipCellFactory implements WritableFactory {
		INSTANCE;
		@Override
		public Writable create() {
			return new SkipCell(-1, EncodingFactory.INSTANCE);
		}
	}

}
