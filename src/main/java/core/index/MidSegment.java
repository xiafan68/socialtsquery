package core.index;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

import segmentation.Segment;

/**
 * posting list里面存储的元素
 * @author xiafan
 *
 */
public class MidSegment extends Segment implements
		WritableComparable<MidSegment> {
	public long mid;

	public MidSegment(long midTmp, Segment seg) {
		super(seg);
		this.mid = midTmp;
	}

	public MidSegment() {

	}

	public void readFields(DataInput input) throws IOException {
		mid = input.readLong();
		super.read(input);
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		arg0.writeLong(mid);
		super.write(arg0);
	}

	public int compareTo(MidSegment o) {
		int maxA = Math.max(getStartCount(), getEndCount());
		int maxB = Math.max(o.getStartCount(), o.getEndCount());
		int ret = Integer.compare(maxB, maxA);
		if (ret == 0) {
			ret = Integer.compare(getStart(), o.getStart());
			if (ret == 0) {
				ret = Long.compare(mid, o.mid);
			}
		}
		return ret;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "MidSegment [mid=" + mid + ", start=" + start + ", count="
				+ count + ", endTime=" + endTime + ", endCount=" + endCount
				+ "]";
	}
}
