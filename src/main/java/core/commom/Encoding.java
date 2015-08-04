package core.commom;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

/**
 * encoding: 1. value of z 2. z value of x and y
 * 
 * @author xiafan
 */
public class Encoding extends Point implements WritableComparable<Encoding> {
	private int paddingBitNum;// the number bits that are padded at the ends
	private int edgeLen = -1;
	private int[] encodes = new int[3];

	public Encoding() {
	}

	public Encoding(Point p, int paddingBitNum) {
		super(p.x, p.y, p.z);
		this.paddingBitNum = paddingBitNum;
		encode();
	}

	private void encode() {
		encodes[0] = z;
		int mask = 1;
		for (int i = 0; i < paddingBitNum; i++) {
			encodes[0] |= mask;
			mask <<= 1;
		}

		int curIdx = 1;
		mask = 1 << 31;
		int zvalueIdx = 63;
		int maskIdx = 31;
		for (int i = 0; i < 32; i++) {
			if (maskIdx < paddingBitNum)
				break;
			curIdx = 2 - zvalueIdx / 32;
			encodes[curIdx] |= (x & mask) >>> maskIdx << (zvalueIdx-- % 32);
			encodes[curIdx] |= (y & mask) >> (maskIdx--) << (zvalueIdx-- % 32);
			mask >>>= 1;
		}
	}

	private void decode() {
		x = 0;
		y = 0;
		z = encodes[0];
		int mask = -1 << 1;
		for (int i = 0; i < paddingBitNum; i++) {
			z &= mask;
			mask <<= 1;
		}
		int zvalueBit = 63;
		mask = 1 << 31;
		// System.out.println(Integer.toBinaryString(mask));
		for (int i = 31; i >= paddingBitNum; i--) {
			int curIdx = 2 - zvalueBit / 32;
			x |= (encodes[curIdx] & mask) >>> (zvalueBit-- % 32) << i;
			mask >>>= 1;
			y |= (encodes[curIdx] & mask) >>> (zvalueBit-- % 32) << i;
			mask >>>= 1;
			if (mask == 0)
				mask = 1 << 31;
		}
	}

	public int getX() {
		return x;
	}

	public void setX(int x) {
		this.x = x;
		encode();
	}

	public int getY() {
		return y;
	}

	public void setY(int y) {
		this.y = y;
		encode();
	}

	public int getZ() {
		return z;
	}

	public void setZ(int z) {
		this.z = z;
		encode();
	}

	public int getTopZ() {
		return encodes[0];
	}

	public void setTop(int zTop) {
		int len = (1 << paddingBitNum) - 1;
		z = zTop - len;
		setZ(z);
	}

	/**
	 * int len = endBit - startBit + 1; len = ((len*3)+7)/8*8; //compute the
	 * number bytes
	 */

	@Override
	public void readFields(DataInput arg0) throws IOException {
		paddingBitNum = arg0.readInt();
		for (int i = 0; i < 3; i++)
			encodes[i] = arg0.readInt();
		decode();
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		arg0.writeInt(paddingBitNum);
		for (int code : encodes)
			arg0.writeInt(code);
	}

	@Override
	public int compareTo(Encoding arg0) {
		int ret = 0;
		ret = 0 - Integer.compare(encodes[0], arg0.encodes[0]);
		for (int i = 1; i < encodes.length; i++) {
			if (ret != 0)
				break;
			ret = Long.compare((encodes[i] & 0xffffffffL), (arg0.encodes[i] & 0xffffffffL));
		}

		return ret;
	}

	@Override
	public String toString() {
		String ret = "HybridEncoding [endBit=" + paddingBitNum + "\n, x=" + getX() + "," + Integer.toBinaryString(x)
				+ "\n, y=" + getY() + "," + Integer.toBinaryString(y) + "\n,z=" + getZ() + ","
				+ Integer.toBinaryString(encodes[0]) + "]\n, encoding:";
		for (int code : encodes) {
			ret += Integer.toBinaryString(code) + ",";
		}
		return ret;
	}

	public static void main(String[] args) throws IOException {
		Encoding data = new Encoding(new Point(234, 123, 23), 2);
		System.out.println(data);
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		DataOutputStream dao = new DataOutputStream(baos);
		data.write(dao);
		Encoding newData = new Encoding();
		newData.readFields(new DataInputStream(new ByteArrayInputStream(baos.toByteArray())));
		System.out.println(newData);
	}

	public int getEdgeLen() {
		return 1 << paddingBitNum;
	}

	public void setEndBits(int i) {
		paddingBitNum = i;
	}
}
