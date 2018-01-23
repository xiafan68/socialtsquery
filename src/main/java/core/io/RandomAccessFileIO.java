package core.io;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;

public class RandomAccessFileIO extends SeekableDirectIO {
	RandomAccessFile file;

	public RandomAccessFileIO(String path) throws FileNotFoundException {
		file = new RandomAccessFile(path, "r");
	}

	public void seek(long pos) throws IOException {
		file.seek(pos);
	}

	public long position() throws IOException {
		return file.getChannel().position();
	}

	/**
	 * TODO:目前实现写出的数据不能够超过BLOCK_SIZE
	 * 
	 * @param buf
	 * @return
	 * @throws IOException
	 */
	public void readFully(byte[] buf) throws IOException {
		file.read(buf);
	}

	/**
	 * @param buf
	 * @return
	 * @throws IOException
	 */
	public final int write(byte[] buf) throws IOException {
		file.write(buf);
		return 0;
	}

	/**
	 * TODO:目前实现写出的数据不能够超过BLOCK_SIZE
	 * 
	 * @param buf
	 * @return
	 * @throws IOException
	 */
	public final int write(ByteBuffer buf) throws IOException {
		file.write(buf.array());
		return 0;
	}

	public final void close() throws IOException {
		file.close();
	}

	@Override
	public void readFully(byte[] b, int off, int len) throws IOException {
		file.readFully(b, off, len);
	}

	@Override
	public int skipBytes(int n) throws IOException {
		return file.skipBytes(n);
	}

	@Override
	public boolean readBoolean() throws IOException {
		return file.readBoolean();
	}

	@Override
	public byte readByte() throws IOException {
		return file.readByte();
	}

	@Override
	public int readUnsignedByte() throws IOException {
		return file.readUnsignedByte();
	}

	@Override
	public short readShort() throws IOException {
		return file.readShort();
	}

	@Override
	public int readUnsignedShort() throws IOException {
		return file.readUnsignedShort();
	}

	@Override
	public char readChar() throws IOException {
		return file.readChar();
	}

	@Override
	public int readInt() throws IOException {
		return file.readInt();
	}

	@Override
	public long readLong() throws IOException {
		return file.readLong();
	}

	@Override
	public float readFloat() throws IOException {
		return file.readFloat();
	}

	@Override
	public double readDouble() throws IOException {
		return file.readDouble();
	}

	@Override
	public String readLine() throws IOException {
		return file.readLine();
	}

	@Override
	public String readUTF() throws IOException {
		return file.readUTF();
	}
}
