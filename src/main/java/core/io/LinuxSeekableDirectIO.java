package core.io;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import com.sun.jna.Native;
import com.sun.jna.Pointer;
import com.sun.jna.ptr.PointerByReference;

/**
 * 
 * @author xiafan
 * @version 0.1 Mar 20, 2015
 */
public class LinuxSeekableDirectIO extends SeekableDirectIO {
	// native c library
	public static final int O_RDONLY = 00;
	public static final int O_WRONLY = 01;
	public static final int O_RDWR = 02;
	public static final int O_CREAT = 0100;
	public static final int O_EXCL = 0200;
	public static final int O_NOCTTY = 0400;
	public static final int O_TRUNC = 01000;
	public static final int O_APPEND = 02000;
	public static final int O_NONBLOCK = 04000;
	public static final int O_NDELAY = O_NONBLOCK;
	public static final int O_SYNC = 010000;
	public static final int O_ASYNC = 020000;
	public static final int O_DIRECT = 040000;
	public static final int O_DIRECTORY = 0200000;
	public static final int O_NOFOLLOW = 0400000;
	public static final int O_NOATIME = 01000000;
	public static final int O_CLOEXEC = 02000000;

	public LinuxSeekableDirectIO(String pathname) throws IOException {
		this(pathname, LinuxSeekableDirectIO.O_RDWR | LinuxSeekableDirectIO.O_DIRECT);
	}

	public LinuxSeekableDirectIO(String pathname, int flags) throws IOException {
		fd = open(pathname, flags, S_IRWXU);
		if (fd < 0) {
			Pointer p = strerror(Native.getLastError());
			throw new IOException(p.getString(0));
		}
		bufPRef = new PointerByReference();
		posix_memalign(bufPRef, BLOCK_SIZE, BLOCK_SIZE);
		bufPointer = bufPRef.getValue();
		memset(bufPointer, BLOCK_SIZE, 0);
	}

	public static void main(String[] test) throws IOException {
		SeekableDirectIO io = new LinuxSeekableDirectIO("/tmp/iotest.txt",
				LinuxSeekableDirectIO.O_RDWR | LinuxSeekableDirectIO.O_DIRECT | LinuxSeekableDirectIO.O_CREAT);
		//io.seek(10);
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		DataOutputStream bos = new DataOutputStream(baos);
		for (int i = 0; i < 100; i++) {
			bos.writeInt(i);
		}
		io.write(baos.toByteArray());
		io.close();
		io = new LinuxSeekableDirectIO("/tmp/iotest.txt",
				LinuxSeekableDirectIO.O_RDWR | LinuxSeekableDirectIO.O_DIRECT | LinuxSeekableDirectIO.O_CREAT);
		//io.seek(10);
		byte[] bytes = new byte[baos.toByteArray().length];
		io.readFully(bytes);
		DataInputStream bis = new DataInputStream(new ByteArrayInputStream(bytes));
		for (int i = 0; i < 100; i++) {
			System.out.println(bis.readInt());
		}
		io.close();
	}
}
