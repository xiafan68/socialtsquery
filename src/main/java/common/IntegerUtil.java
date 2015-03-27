package common;

public class IntegerUtil {

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
}
