package core.lsmt;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public interface IndexKey extends Comparable<IndexKey> {
	public void write(DataOutput output) throws IOException;

	public void read(DataInput input) throws IOException;

	public static interface IndexKeyFactory {
		IndexKey createIndexKey();
	}
}
