package core.lsmo;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import core.io.Bucket.BucketID;
import core.lsmt.DirEntry;

public class MarkDirEntry extends DirEntry {
	public BucketID startMarkOffset = new BucketID(0, (short) 0);
	public int markNum = 0;

	public MarkDirEntry() {

	}

	public MarkDirEntry(DirEntry curDir) {
		super(curDir);
	}

	@Override
	public void write(DataOutput output) throws IOException {
		super.write(output);
		startMarkOffset.write(output);
		output.writeInt(markNum);
	}

	@Override
	public void read(DataInput input) throws IOException {
		super.read(input);
		startMarkOffset.read(input);
		markNum = input.readInt();
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (!super.equals(obj))
			return false;
		if (getClass() != obj.getClass())
			return false;
		MarkDirEntry other = (MarkDirEntry) obj;
		if (markNum != other.markNum)
			return false;
		if (startMarkOffset == null) {
			if (other.startMarkOffset != null)
				return false;
		} else if (!startMarkOffset.equals(other.startMarkOffset))
			return false;
		return true;
	}

}