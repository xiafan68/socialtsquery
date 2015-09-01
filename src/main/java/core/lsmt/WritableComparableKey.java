package core.lsmt;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Comparator;

public interface WritableComparableKey extends Comparable<WritableComparableKey> {
	public void write(DataOutput output) throws IOException;

	public void read(DataInput input) throws IOException;

	public static enum WritableComparableKeyComp implements Comparator<WritableComparableKey> {
		INSTANCE;
		@Override
		public int compare(WritableComparableKey o1, WritableComparableKey o2) {
			return o1.compareTo(o2);
		}
	}

	public static interface WritableComparableKeyFactory {
		WritableComparableKey createIndexKey();
	}

	public static enum StringKeyFactory implements WritableComparableKeyFactory {
		INSTANCE;
		public WritableComparableKey createIndexKey() {
			return new StringKey();
		}
	}

	public static class StringKey implements WritableComparableKey {
		String val;

		public StringKey(String val) {
			this.val = val;
		}

		public StringKey() {

		}

		@Override
		public int compareTo(WritableComparableKey o) {
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
	}
}
