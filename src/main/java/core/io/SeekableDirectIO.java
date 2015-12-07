package core.io;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

import com.sun.jna.Native;
import com.sun.jna.NativeLong;
import com.sun.jna.Platform;
import com.sun.jna.Pointer;
import com.sun.jna.ptr.PointerByReference;

/**
 * 基于DirectIO的java实现的父类，之所以这样是由于不同的平台上打开文件的函数，传给函数的参数值均可能不同
 * 
 * @author xiafan
 * 
 */
public abstract class SeekableDirectIO implements DataInput {
	public static final int BLOCK_SIZE = 4096;
	// seek arguments
	public static final int SEEK_SET = 0;
	public static final int SEEK_CUR = 1;
	public static final int SEEK_END = 2;

	// create mode
	public static final int S_IRWXU = 00700;

	// public static final int BLOCK_SIZE = 4096;
	static {
		try {
			System.out.println("os type:" + Platform.getOSType());
			Native.register(Platform.C_LIBRARY_NAME);
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}

	// read the string repr of errno
	protected native Pointer strerror(int errno);

	protected native int fcntl(int fd, int cmd, int arg);

	// file related api
	protected native int open(String pathname, int flags, int mode);

	protected native NativeLong read(int fd, Pointer buf, NativeLong count);

	protected native NativeLong write(int fd, Pointer buf, NativeLong count);

	protected native NativeLong lseek(int fd, NativeLong offset, int whence);

	protected native int close(int fd);

	// memory related api
	protected native int posix_memalign(PointerByReference memptr, int alignment, int size);

	protected native Pointer memset(Pointer p, int value, NativeLong size);

	protected native void free(Pointer p);

	protected int fd;
	protected Pointer bufPointer;
	protected PointerByReference bufPRef;

	public static SeekableDirectIO create(String path) throws IOException {
		SeekableDirectIO ret = null;
		// return new RandomAccessFileIO(path);
		String os = System.getProperty("os.name").toLowerCase();
		if (os.contains("linux")) {
			ret = new LinuxSeekableDirectIO(path);
		} else if (os.contains("mac")) {
			ret = new MacSeekableDirectIO(path);
		} else {
			throw new RuntimeException("unsupported system for directio");
		}
		// ret = new RandomAccessFileIO(path);
		return ret;
	}

	public static SeekableDirectIO create(File path, String mode) throws IOException {
		return create(path.getAbsolutePath(), mode);
	}

	public static SeekableDirectIO create(String path, String mode) throws IOException {
		return create(path);
	}

	byte[] oneByte = new byte[1];

	protected int read() throws IOException {
		readFully(oneByte);
		return oneByte[0];
	}

	public void seek(long pos) throws IOException {
		lseek(fd, new NativeLong( pos), SEEK_SET);
	}

	public long position() throws IOException {
		return lseek(fd, new NativeLong(0), SEEK_CUR).longValue();
	}

	/**
	 * TODO:目前实现写出的数据不能够超过BLOCK_SIZE
	 * 
	 * @param buf
	 * @return
	 * @throws IOException
	 */
	public void readFully(byte[] buf) throws IOException {
		int rtn = 0;
		int cur = 0;
		int len = 0;
		int remain = buf.length;
		while (remain > 0) {
			len = Math.min(remain, BLOCK_SIZE);
			rtn += read(fd, bufPointer, new NativeLong(len)).longValue();
			bufPointer.read(0, buf, cur, len);
			cur += len;
			remain -= len;
		}

		// return rtn;
	}

	/**
	 * TODO:目前实现写出的数据不能够超过BLOCK_SIZE
	 * 
	 * @param buf
	 * @return
	 * @throws IOException
	 */
	public int read(ByteBuffer byteBuffer) throws IOException {
		int rtn = 0;
		int cur = 0;
		int len = 0;
		byte[] buf = byteBuffer.array();
		int remain = buf.length;
		while (remain > 0) {
			len = Math.min(remain, BLOCK_SIZE);
			rtn += read(fd, bufPointer, new NativeLong(len)).longValue();
			bufPointer.read(0, buf, cur, len);
			cur += len;
			remain -= len;
		}

		return rtn;
	}

	/**
	 * @param buf
	 * @return
	 * @throws IOException
	 */
	public int write(byte[] buf) throws IOException {
		int rtn = 0;
		int cur = 0;
		int len = 0;
		int remain = buf.length;
		while (remain > 0) {
			len = Math.min(remain, BLOCK_SIZE);
			bufPointer.write(0, buf, cur, len);
			rtn += write(fd, bufPointer, new NativeLong(len)).longValue();
			cur += len;
			remain -= len;
		}
		return rtn;
	}

	/**
	 * TODO:目前实现写出的数据不能够超过BLOCK_SIZE
	 * 
	 * @param buf
	 * @return
	 * @throws IOException
	 */
	public int write(ByteBuffer buf) throws IOException {
		return write(buf.array());
	}

	public void close() throws IOException {
		if (close(fd) < 0)
			throw new IOException("Problems occured while doing close()");
	}

	@Override
	public void readFully(byte[] b, int off, int len) throws IOException {
		int cur = off;
		int remain = len;
		while (remain > 0) {
			len = Math.min(remain, BLOCK_SIZE);
			read(fd, bufPointer, new NativeLong(len));
			bufPointer.read(0, b, cur, len);
			cur += len;
			remain -= len;
		}
	}

	@Override
	public int skipBytes(int n) throws IOException {
		return  lseek(fd, new NativeLong(n), SEEK_CUR).intValue();
	}

	@Override
	public boolean readBoolean() throws IOException {
		int ch = this.read();
		if (ch < 0)
			throw new EOFException();
		return (ch != 0);
	}

	@Override
	public byte readByte() throws IOException {
		int ch = this.read();
		if (ch < 0)
			throw new EOFException();
		return (byte) ch;
	}

	@Override
	public int readUnsignedByte() throws IOException {
		int ch = this.read();
		if (ch < 0)
			throw new EOFException();
		return (byte) ch;
	}

	@Override
	public short readShort() throws IOException {
		int ch1 = this.read();
		int ch2 = this.read();
		if ((ch1 | ch2) < 0)
			throw new EOFException();
		return (short) ((ch1 << 8) + (ch2 << 0));
	}

	@Override
	public int readUnsignedShort() throws IOException {
		int ch1 = this.read();
		int ch2 = this.read();
		if ((ch1 | ch2) < 0)
			throw new EOFException();
		return (ch1 << 8) + (ch2 << 0);
	}

	@Override
	public char readChar() throws IOException {
		int ch1 = this.read();
		int ch2 = this.read();
		if ((ch1 | ch2) < 0)
			throw new EOFException();
		return (char) ((ch1 << 8) + (ch2 << 0));
	}

	@Override
	public int readInt() throws IOException {
		int ch1 = this.read();
		int ch2 = this.read();
		int ch3 = this.read();
		int ch4 = this.read();
		if ((ch1 | ch2 | ch3 | ch4) < 0)
			throw new EOFException();
		return ((ch1 << 24) + (ch2 << 16) + (ch3 << 8) + (ch4 << 0));
	}

	@Override
	public long readLong() throws IOException {
		return ((long) (readInt()) << 32) + (readInt() & 0xFFFFFFFFL);
	}

	@Override
	public float readFloat() throws IOException {
		return Float.intBitsToFloat(readInt());
	}

	@Override
	public double readDouble() throws IOException {
		return Double.longBitsToDouble(readLong());
	}

	@Override
	public String readLine() throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String readUTF() throws IOException {
		return DataInputStream.readUTF(this);
	}

}
