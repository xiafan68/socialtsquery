package core.commom;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;

import core.lsmt.WritableComparableKey;
import io.ByteUtil;

/**
 * encoding: 1. value of z 2. z value of x and y
 * 
 * @author xiafan
 */
public class Encoding extends Point implements WritableComparableKey {
	// log_2(length of cube)
	private int paddingBitNum;// the number bits that are padded at the ends
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

		encodes[1] = 0;
		encodes[2] = 0;
		int curIdx = 1;
		mask = 1 << 31;
		int zvalueIdx = 63;
		int maskIdx = 31;
		for (int i = 0; i < 32; i++) {
			if (maskIdx < paddingBitNum)
				break;
			curIdx = 2 - zvalueIdx / 32;
			encodes[curIdx] |= (x & mask) >>> maskIdx << (zvalueIdx-- % 32);
			encodes[curIdx] |= (y & mask) >>> (maskIdx--) << (zvalueIdx-- % 32);
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

	/**
	 * 这个topZ其实是z+edgeLen-1的结果，这个edgeLen是2的次幂
	 * 
	 * @return
	 */
	public int getTopZ() {
		return encodes[0];
	}

	/**
	 * 这里提供的zTop应该要同上？
	 * 
	 * @param zTop
	 */
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
	public void read(DataInput input) throws IOException {
		paddingBitNum = ByteUtil.readVInt(input);
		for (int i = 0; i < 3; i++) {
			// encodes[i] = ByteUtil.readVInt(input);
			encodes[i] = input.readInt();
		}
		decode();
	}

	@Override
	public void write(DataOutput output) throws IOException {
		ByteUtil.writeVInt(output, paddingBitNum);
		for (int code : encodes) {
			// ByteUtil.writeVInt(output, code);
			output.writeInt(code);
		}
	}

	@Override
	public int compareTo(WritableComparableKey other) {
		Encoding arg0 = (Encoding) other;
		int ret = 0;
		ret = 0 - Integer.compare(encodes[0], arg0.encodes[0]);
		for (int i = 1; i < encodes.length; i++) {
			if (ret != 0)
				break;
			ret = Long.compare((encodes[i] & 0xffffffffL), (arg0.encodes[i] & 0xffffffffL));
		}
		if (ret == 0)
			ret = 0 - Integer.compare(paddingBitNum, arg0.paddingBitNum);
		return ret;
	}

	@Override
	public boolean equals(Object object) {
		if (!(object instanceof Encoding)) {
			return false;
		}
		Encoding oCode = (Encoding) object;
		return x == oCode.x && y == oCode.y && z == oCode.z && paddingBitNum == oCode.paddingBitNum;
	}

	@Override
	public int hashCode() {
		return x + y + z + paddingBitNum;
	};

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
		newData.read(new DataInputStream(new ByteArrayInputStream(baos.toByteArray())));

		System.out.println(newData);

		data = new Encoding(new Point(0, 696602, 1), 0);
		System.out.println(data);
	}

	public int getEdgeLen() {
		return 1 << paddingBitNum;
	}

	public void setPaddingBitNum(int i) {
		paddingBitNum = i;
	}

	public int getPaddingBitNum() {
		return paddingBitNum;
	}

	public boolean contains(Encoding curMin) {
		if (getX() <= curMin.getX() && getX() + getEdgeLen() >= curMin.getX() + curMin.getEdgeLen()
				&& getY() <= curMin.getY() && getY() + getEdgeLen() >= curMin.getY() + curMin.getEdgeLen()
				&& getZ() <= curMin.getZ() && getTopZ() >= curMin.getTopZ() + curMin.getEdgeLen())
			return true;
		return false;
	}
	
	/**
	 * 判断当前cube是否为其父节点的第一个子节点，第一个子叶节点无论是否为空，也要写出到磁盘中
	 * 
	 * @param code
	 * @return
	 */
	public static boolean isMarkupNode(Encoding code) {
		//suppose padding bit num is 2,then values of x,y and z could be xx000,xx100. By testing the padding th bit, we 
		//can know whether current node is a mark up node of its parent
		int mark = 1 << code.getPaddingBitNum();
		boolean ret = (code.getZ() & mark) != 0;
		ret = ret & ((code.getX() & mark) == 0);
		ret = ret & ((code.getY() & mark) == 0);
		return ret;
	}
	


	/**
	 * 判断当前childEncoding是否为其父节点的第一个子节点，第一个子叶节点无论是否为空，也要写出到磁盘中
	 * 使用isMarkupNode
	 * @param code
	 * @return
	 */
	@Deprecated
	public static boolean isMarkupNodeOfParent(Encoding childEncoding, Encoding parentEncoding) {
		return childEncoding.getTopZ() == parentEncoding.getTopZ() && childEncoding.getX() == parentEncoding.getX()
				&& childEncoding.getY() == parentEncoding.getY();

	}
}
