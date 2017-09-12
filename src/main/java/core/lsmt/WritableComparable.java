package core.lsmt;

import core.commom.Encoding;
import core.lsmi.ListDiskSSTableReader;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Comparator;

public interface WritableComparable extends Comparable<WritableComparable>, Writable {

	public static enum WritableComparableKeyComp implements Comparator<WritableComparable> {
		INSTANCE;
		@Override
		public int compare(WritableComparable o1, WritableComparable o2) {
			return o1.compareTo(o2);
		}
	}

	public static interface WritableComparableKeyFactory {
		WritableComparable createIndexKey();
	}
	//
	// public static enum SegKeyList WritableComparableKeyFactory {
	// WritableComparableKey createIndexKey();
	// }

	public static enum StringKeyFactory implements WritableComparableKeyFactory {
		INSTANCE;
		public WritableComparable createIndexKey() {
			return new StringKey();
		}
	}

	public static enum EncodingFactory implements WritableComparableKeyFactory {
		INSTANCE;
		public WritableComparable createIndexKey() {
			return new Encoding();
		}
	}

	public static enum SegListKeyFactory implements WritableComparableKeyFactory {
		INSTANCE;
		public WritableComparable createIndexKey() {
			return new ListDiskSSTableReader.SegListKey();
		}
	}

	public static class StringKey implements WritableComparable {
		String val;

		public StringKey(String val) {
			this.val = val;
		}

		public StringKey() {

		}

		@Override
		public int compareTo(WritableComparable o) {
			StringKey other = (StringKey) o;
			return val.compareTo(other.val);
		}

		@Override
		public void write(DataOutput output) throws IOException {
			output.writeUTF(val);
		}

		@Override
		public void read(DataInput input) throws IOException {
			val = input.readUTF();
		}

		@Override
		public int hashCode() {
			return val.hashCode();
		}

		@Override
		public boolean equals(Object object) {
			if (this == object)
				return true;
			StringKey o = (StringKey) object;
			return val.equals(o.val);
		}

		@Override
		public String toString() {
			return val;
		}
	}
}
