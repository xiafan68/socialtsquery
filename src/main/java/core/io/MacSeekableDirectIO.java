package core.io;

import java.io.IOException;

import com.sun.jna.Native;
import com.sun.jna.Pointer;
import com.sun.jna.ptr.PointerByReference;

public class MacSeekableDirectIO extends SeekableDirectIO {
	// native c library
	public static final int O_RDONLY = 00;
	public static final int O_WRONLY = 01;
	public static final int O_RDWR = 0x0002;

	public static final int O_APPEND = 0x0008;
	public static final int O_NONBLOCK = 0x0004;

	public static final int O_CREAT = 0x0200;
	public static final int O_EXCL = 0x0800;
	public static final int O_NOCTTY = 0;
	public static final int O_TRUNC = 0x0400;

	public static final int O_NDELAY = O_NONBLOCK;
	public static final int O_SYNC = 0x0080;
	public static final int O_ASYNC = 0x0040;

	public static final int O_DIRECTORY = 0200000;
	public static final int O_NOFOLLOW = 0400000;
	public static final int O_NOATIME = 01000000;
	public static final int O_CLOEXEC = 02000000;

	// fcntl options
	// https://github.com/phracker/MacOSX-SDKs/tree/master/MacOSX10.4u.sdk/System/Library/Frameworks/Kernel.framework/Versions/A/Headers/sys
	public static final int F_NOCACHE = 48;

	public MacSeekableDirectIO(String pathname) throws IOException {
		this(pathname, MacSeekableDirectIO.O_RDWR | MacSeekableDirectIO.O_CREAT);
	}

	public MacSeekableDirectIO(String pathname, int flags) throws IOException {
		fd = open(pathname, flags, S_IRWXU);
		if (fd < 0) {
			int errno = Native.getLastError();
			String err = "unknow error";
			if (errno != 0) {
				Pointer p = strerror(errno);
				err = p.getString(0);
			}
			throw new IOException(err);
		}

		fcntl(fd, F_NOCACHE, 0);

		bufPRef = new PointerByReference();
		posix_memalign(bufPRef, BLOCK_SIZE, BLOCK_SIZE);
		bufPointer = bufPRef.getValue();
	}
}
