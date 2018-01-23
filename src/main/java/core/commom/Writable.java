package core.commom;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public interface Writable {
	public void write(DataOutput output) throws IOException;

	public void read(DataInput input) throws IOException;
}
