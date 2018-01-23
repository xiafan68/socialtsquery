package common;

public class IntegerUtil {

	/**
	 * assume data is non-negative
	 * @param data
	 * @return
	 */
	public static int firstNoneZero(int data) {
		int i = 31;
		int mask = 1 << i;
		for (; i >= 0; i--) {
			if ((mask & data) != 0)
				break;
			mask >>= 1;
		}
		return i;
	}

	public static void writeInt(int data, byte[] bytes, int start) {
		bytes[start++] = (byte) ((data >> 24) & (0xff));
		bytes[start++] = (byte) ((data >> 16) & (0xff));
		bytes[start++] = (byte) ((data >> 8) & (0xff));
		bytes[start++] = (byte) (data & (0xff));
	}

	public static int readInt(byte[] bytes, int start) {
		int ret = 0;
		ret = ret | ((bytes[start++] & 0xff) << 24);
		ret = ret | ((bytes[start++] & 0xff) << 16);
		ret = ret | ((bytes[start++] & 0xff) << 8);
		ret = ret | (bytes[start++] & 0xff);
		return ret;
	}

	public static byte[] encodeLong(long data) {
		byte[] buf;
		int size = 1;
		long cp = data >> 8;
		while (cp > 0x1F) {
			size++;
		}
		buf = new byte[size];
		/*
		if (data <= 0x1F) {
			buf = new byte[1];
		} else if (data <= 0x1FFF) {
			buf = new byte[2];
		} else if (data <= 0x1FFFFF) {
			buf = new byte[3];
		} else if (data <= 0x1FFFFFFF) {
			buf = new byte[4];
		} else if (data <= 0x1FFFFFFFFFl) {
			buf = new byte[5];
		} else if (data <= 0x1FFFFFFFFFFFl) {
			buf = new byte[6];
		} else if (data <= 0x1FFFFFFFFFFFFFl) {
			buf = new byte[7];
		} else if (data <= 0x1FFFFFFFffffFFFFl) {
			buf = new byte[8];
		} else {
			buf = new byte[9];
		}*/

		int offset = 0;
		for (int i = 1; i < buf.length; i++) {
			buf[buf.length - i] = (byte) (((data) >> offset) & 0xFF);
			offset += 8;
		}
		buf[0] = (byte) (((data >> offset) & 0xFF) | (((buf.length - 1) << 5) & 0xE0));
		return buf;
	}

	public static long decodeLong(byte[] bytes, int start, int[] offset) {
		long ret = 0;
		offset[0] = ((bytes[start] >> 5) & 0x07) + 1;
		int offsetNum = 0;
		for (int i = offset[0] - 1; i > 0; i--) {
			ret |= (long) (bytes[start + i] & 0xFFl) << offsetNum;
			offsetNum += 8;
		}
		ret |= ((bytes[start] & 0x1Fl) << offsetNum);
		return ret;
	}
}
