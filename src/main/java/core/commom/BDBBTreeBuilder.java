package core.commom;

import java.io.File;

import core.commom.WritableComparable.WritableComparableFactory;

public class BDBBTreeBuilder {
	private File dir;

	private WritableComparableFactory keyFactory;

	private WritableComparableFactory secondaryKeyFactory;

	private WritableFactory valueFactory;

	private boolean readOnly;

	private boolean allowDuplicates;

	public static BDBBTreeBuilder create() {
		return new BDBBTreeBuilder();
	}

	public BDBBtree build() {
		return new BDBBtree(this);
	}

	public WritableComparableFactory getKeyFactory() {
		return keyFactory;
	}

	public BDBBTreeBuilder setKeyFactory(WritableComparableFactory keyFactory) {
		this.keyFactory = keyFactory;
		return this;
	}

	public WritableComparableFactory getSecondaryKeyFactory() {
		return secondaryKeyFactory;
	}

	public BDBBTreeBuilder setSecondaryKeyFactory(WritableComparableFactory secondaryKeyFactory) {
		this.secondaryKeyFactory = secondaryKeyFactory;
		return this;
	}

	public WritableFactory getValueFactory() {
		return valueFactory;
	}

	public BDBBTreeBuilder setValueFactory(WritableFactory valueFactory) {
		this.valueFactory = valueFactory;
		return this;
	}

	public boolean isReadOnly() {
		return readOnly;
	}

	public BDBBTreeBuilder setReadOnly(boolean readOnly) {
		this.readOnly = readOnly;
		return this;
	}

	public boolean isAllowDuplicates() {
		return allowDuplicates;
	}

	public BDBBTreeBuilder setAllowDuplicates(boolean allowDuplicates) {
		this.allowDuplicates = allowDuplicates;
		return this;
	}

	public File getDir() {
		return dir;
	}

	public BDBBTreeBuilder setDir(File dir) {
		this.dir = dir;
		return this;
	}
}
