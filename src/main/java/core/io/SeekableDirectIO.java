package core.io;

import java.io.DataInput;
import java.io.IOException;
import java.nio.ByteBuffer;

import com.sun.jna.Native;
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
		Native.register("c");
	}

	// read the string repr of errno
	protected native Pointer strerror(int errno);

	protected native int fcntl(int fd, int cmd, int arg);

	// file related api
	protected native int open(String pathname, int flags, int mode);

	protected native int read(int fd, Pointer buf, int count);

	protected native int write(int fd, Pointer buf, int count);

	protected native int lseek(int fd, long offset, int whence);

	protected native int lseek(int fd, int offset, int whence);

	protected native int close(int fd);

	// memory related api
	protected native int posix_memalign(PointerByReference memptr, int alignment, int size);

	protected native void free(Pointer p);

	protected int fd;
	protected Pointer bufPointer;
	protected PointerByReference bufPRef;

	public void seek(long pos) throws IOException {
		lseek(fd, pos, SEEK_SET);
	}

	public long seek() {
		return lseek(fd, 0, SEEK_CUR);
	}

	/**
	 * TODO:目前实现写出的数据不能够超过BLOCK_SIZE
	 * 
	 * @param buf
	 * @return
	 * @throws IOException
	 */
	public final void readFully(byte[] buf) throws IOException {
		int rtn = 0;
		int cur = 0;
		int len = 0;
		int remain = buf.length;
		while (remain > 0) {
			len = Math.min(remain, BLOCK_SIZE);
			rtn += read(fd, bufPointer, BLOCK_SIZE);
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
	public final int read(ByteBuffer byteBuffer) throws IOException {
		int rtn = 0;
		int cur = 0;
		int len = 0;
		byte[] buf = byteBuffer.array();
		int remain = buf.length;
		while (remain > 0) {
			len = Math.min(remain, BLOCK_SIZE);
			rtn += read(fd, bufPointer, BLOCK_SIZE);
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
	public final int write(byte[] buf) throws IOException {
		int rtn = 0;
		int cur = 0;
		int len = 0;
		int remain = buf.length;
		while (remain > 0) {
			len = Math.min(remain, BLOCK_SIZE);
			bufPointer.write(0, buf, cur, len);
			rtn += write(fd, bufPointer, BLOCK_SIZE);
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
	public final int write(ByteBuffer buf) throws IOException {
		return write(buf.array());
	}

	public final void close() throws IOException {
		if (close(fd) < 0)
			throw new IOException("Problems occured while doing close()");
	}

	public static SeekableDirectIO create(String path) throws IOException {
		String os = System.getProperty("os.name").toLowerCase();
		if (os.contains("linux")) {
			return new LinuxSeekableDirectIO(path);
		} else if (os.contains("mac")) {
			return new MacSeekableDirectIO(path);
		} else {
			throw new RuntimeException("unsupported system for directio");
		}
	}

	@Override
	public void readFully(byte[] b, int off, int len) throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public int skipBytes(int n) throws IOException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public boolean readBoolean() throws IOException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public byte readByte() throws IOException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int readUnsignedByte() throws IOException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public short readShort() throws IOException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int readUnsignedShort() throws IOException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public char readChar() throws IOException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int readInt() throws IOException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public long readLong() throws IOException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public float readFloat() throws IOException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public double readDouble() throws IOException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public String readLine() throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String readUTF() throws IOException {
		// TODO Auto-generated method stub
		return null;
	}
}
