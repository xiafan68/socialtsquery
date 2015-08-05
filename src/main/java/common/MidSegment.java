package common;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

import segmentation.Segment;
import core.commom.Point;

/**
 * posting list里面存储的元素
 * 
 * @author xiafan
 *
 */
public class MidSegment extends Segment implements WritableComparable<MidSegment> {
	public long mid;

	public MidSegment(long midTmp, Segment seg) {
		super(seg);
		this.mid = midTmp;
	}

	public MidSegment() {

	}

	@Override
	public void readFields(DataInput input) throws IOException {
		mid = input.readLong();
		super.read(input);
		/*if (mid == -6723079032l && getStart() == 696962){
			System.out.println();
		}*/
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		arg0.writeLong(mid);
		super.write(arg0);
	}

	@Override
	public boolean parse(String line) {
		String[] fields = line.split("\t");
		mid = Long.parseLong(fields[0]);
		return super.parse(fields[1]);
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

	@Override
	public boolean equals(Object o) {
		MidSegment seg = (MidSegment) o;
		return this.compareTo(seg) == 0;
	}

	@Override
	public int hashCode() {
		return (int) mid;
	}

	public Point getPoint() {
		return new Point(this.getStart(), this.getEndTime(), Math.max(this.getStartCount(), this.getEndCount()));
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "MidSegment [mid=" + mid + ", start=" + start + ", count=" + count + ", endTime=" + endTime
				+ ", endCount=" + endCount + "]";
	}

	public long getMid() {
		return mid;
	}
}
