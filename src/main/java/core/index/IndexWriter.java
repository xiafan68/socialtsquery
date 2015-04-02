package core.index;

import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

/**
 * 所以的一个partition的写出类
 * 
 * @author xiafan
 * 
 */
public class IndexWriter {
	public static final String IDX_SUFFIX = "idx";
	public static final String BMETA_SUFFIX = "bmeta";
	public static final String DIR_SUFFIX = "dir";

	private static final Logger logger = Logger.getLogger(IndexWriter.class);

	protected FSDataOutputStream directoryWriter;
	protected FSDataOutputStream invIndexWriter;
	protected FSDataOutputStream metaBlockWriter;

	// 当前使用的block
	protected Block block = new Block();
	// 记录posting list每个block中的数据的其实时间，结束时间以及元素个数
	protected BlockMeta curBMeta = new BlockMeta(0, Integer.MAX_VALUE, 0);

	protected DirEntry curEntry;

	protected ByteArrayOutputStream baos = new ByteArrayOutputStream();

	public void open(Configuration conf, Path dir, String part)
			throws IOException {
		invIndexWriter = FileSystem.get(conf).create(
				new Path(dir, part + "." + IDX_SUFFIX), true);
		metaBlockWriter = FileSystem.get(conf).create(
				new Path(dir, part + "." + BMETA_SUFFIX), true);
		directoryWriter = FileSystem.get(conf).create(
				new Path(dir, part + "." + DIR_SUFFIX), true);
	}

	public static class DirEntry {
		public String keyword;
		public long indexPos;
		public long metaPos;
		public int size = 0;
		// 记录block的个数
		public int blockNum = 0;

		public void write(DataOutput output) throws IOException {
			output.writeUTF(keyword);
			output.writeLong(indexPos);
			output.writeLong(metaPos);
			output.writeInt(size);
		}

		public void read(DataInput input) throws IOException {
			keyword = input.readUTF();
			indexPos = input.readLong();
			metaPos = input.readLong();
			size = input.readInt();
		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see java.lang.Object#toString()
		 */
		@Override
		public String toString() {
			return "DirEntry [keyword=" + keyword + ", indexPos=" + indexPos
					+ ", metaPos=" + metaPos + ", size=" + size + ", blockNum="
					+ blockNum + "]";
		}

	}

	public static class BlockMeta {
		public int recNum;
		public int startTime;
		public int endTime;

		public BlockMeta(int recNum, int startTime, int endTime) {
			super();
			this.recNum = recNum;
			this.startTime = startTime;
			this.endTime = endTime;
		}

		public void write(DataOutput output) throws IOException {
			output.writeInt(recNum);
			output.writeInt(startTime);
			output.write(endTime);
		}

		public void read(DataInput input) throws IOException {
			recNum = input.readInt();
			startTime = input.readInt();
			endTime = input.readInt();
		}

		@Override
		public String toString() {
			return "BlockMeta [recNum=" + recNum + ", startTime=" + startTime
					+ ", endTime=" + endTime + "]";
		}

		public void init() {
			recNum = 0;
			startTime = Integer.MAX_VALUE;
			endTime = 0;
		}
	}

	public void newTerm(String keyword) throws IOException {
		curEntry = new DirEntry();
		curEntry.keyword = keyword;
		curEntry.indexPos = invIndexWriter.getPos();
		curEntry.metaPos = metaBlockWriter.getPos();
	}

	public void writeDirEntry() throws IOException {
		curEntry.write(directoryWriter);
	}

	public void writeToIndex(String string, MidSegment value) {
		// logger.info(string + ": " + value);
		DataOutputStream dosTemp = new DataOutputStream(baos);
		try {
			value.write(dosTemp);
			byte[] curData = baos.toByteArray();
			if (!block.write(curData)) {
				writeBlock();
				block.write(curData);
			}
			baos.reset();
		} catch (IOException e) {
		}

		if (value.getStart() < curBMeta.startTime) {
			curBMeta.startTime = value.getStart();
		}
		if (value.getEndTime() > curBMeta.endTime) {
			curBMeta.endTime = value.getEndTime();
		}
	}

	public void writeToIndex(String keyword, Collection<MidSegment> set)
			throws IOException {
		writeToIndex(keyword, set.iterator());
	}

	public void writeToIndex(String keyword, Iterator<MidSegment> iter)
			throws IOException {
		newTerm(keyword);
		while (iter.hasNext()) {
			writeToIndex(keyword, iter.next());
			curEntry.size++;
		}
		writeBlock();
		writeDirEntry();
	}

	/**
	 * TODO:metaBLockWriter文件应该也要以block的方式组织数据，这样便于扩展和管理
	 * @throws IOException
	 */
	private void writeBlock() throws IOException {
		// write block level meta
		if (block.getRecs() != 0) {
			curBMeta.write(metaBlockWriter);
			block.write(invIndexWriter);
			curEntry.blockNum++;

			curBMeta.init();
			block.reset();
		}
	}

	public void close() throws IOException {
		directoryWriter.close();
		invIndexWriter.close();
		metaBlockWriter.close();
	}
}
