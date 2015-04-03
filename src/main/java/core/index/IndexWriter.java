package core.index;

import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

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
	public static final String DIR_SUFFIX = "dir";

	private static final Logger logger = Logger.getLogger(IndexWriter.class);

	protected FSDataOutputStream directoryWriter;

	/**
	 * .idx文件的格式如下： [head block] [[block metas block] [data
	 * block]{1,Block.SIZE/sizeof(BlockMeta)}]* [head
	 * block]:每个idx文件的第一个block是head block [block metas
	 * block]:用于记录接下来1到Block.SIZE/sizeof(BlockMeta)个blocks的相关元数据，
	 * 通过记录的block元数据，可能会直接跳跃到之后的某个block [data block]:用于存储数据的block
	 */
	protected FSDataOutputStream idxIndexWriter;

	// 当前使用的block
	protected Block dataBlock;
	protected int curBlockIdx = 2;// start from the third block
	private List<Block> blocks = new ArrayList<Block>();
	/**
	 * 由于hdfs不支持随机写，这里只能先在内存中缓存部分block了
	 */
	protected Block metaBlock = new Block(Block.META_BLOCK);

	// 记录posting list每个block中的数据的其实时间，结束时间以及元素个数
	protected BlockMeta curBMeta = new BlockMeta(0, Integer.MAX_VALUE, 0);
	protected DirEntry curEntry;

	protected ByteArrayOutputStream baos = new ByteArrayOutputStream();
	DataOutputStream memoryOutput = new DataOutputStream(baos);

	/**
	 * 目前不支持更新和追加操作
	 * 
	 * @param conf
	 * @param dir
	 * @param part
	 * @throws IOException
	 */
	public void open(Configuration conf, Path dir, String part)
			throws IOException {
		idxIndexWriter = FileSystem.get(conf).create(
				new Path(dir, part + "." + IDX_SUFFIX), true);
		directoryWriter = FileSystem.get(conf).create(
				new Path(dir, part + "." + DIR_SUFFIX), true);

		// init the header
		Block head = new Block(Block.HEADER_BLOCK);
		head.write("temporal inverted index 0.2".getBytes());
		head.write(idxIndexWriter);
	}

	public static class DirEntry {
		public String keyword;
		public int dataBlockIdx; // 对应的posting list的第一个block
		public int recNum = 0; // 存储的记录条数
		// 记录block的个数
		public int blockNum = 0; // posting list对应的block数

		public void write(DataOutput output) throws IOException {
			output.writeUTF(keyword);
			output.writeInt(dataBlockIdx);
			output.writeInt(recNum);
			output.writeInt(blockNum);
		}

		public void read(DataInput input) throws IOException {
			keyword = input.readUTF();
			dataBlockIdx = input.readInt();
			recNum = input.readInt();
			blockNum = input.readInt();
		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see java.lang.Object#toString()
		 */
		@Override
		public String toString() {
			return "DirEntry [keyword=" + keyword + ", indexPos="
					+ dataBlockIdx + ", size=" + recNum + ", blockNum="
					+ blockNum + "]";
		}

	}

	public static class BlockMeta {
		public static final int SIZE = 12;
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
			output.writeInt(endTime);
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

	/**
	 * 计算第dataBlockIdx个block的元数据应该存放的block
	 * 
	 * @param dataBlockIdx
	 * @return
	 */
	public static int bMetaBlockIdx(int dataBlockIdx) {
		int idx = (dataBlockIdx + numOfMetasPerBlock())
				/ (numOfMetasPerBlock() + 1);

		return (idx - 1) * (numOfMetasPerBlock() + 1) + 1;
	}

	/**
	 * 
	 * @return the idx of the newed data block
	 * @throws IOException
	 */
	private int newDataBlock() throws IOException {
		if ((curBlockIdx - 1) % (numOfMetasPerBlock() + 1) == 0) {
			curBlockIdx++;
		}
		dataBlock = new Block(Block.DATA_BLOCK);
		return curBlockIdx++;
	}

	public static int nextDataBlockIdx(int blockIdx) {
		blockIdx++;
		if ((blockIdx - 1) % (numOfMetasPerBlock() + 1) == 0) {
			blockIdx++;
		}
		return blockIdx;
	}

	public void flush() throws IOException {
		if (!blocks.isEmpty()) {
			metaBlock.write(idxIndexWriter);
			for (Block block : blocks) {
				block.write(idxIndexWriter);
			}
			metaBlock.reset();
			blocks.clear();
		}
	}

	public void newTerm(String keyword) throws IOException {
		curEntry = new DirEntry();
		curEntry.keyword = keyword;
		curEntry.dataBlockIdx = newDataBlock();
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
			baos.reset();
			if (!dataBlock.write(curData)) {
				writeBlock();
				newDataBlock();
				dataBlock.write(curData);
			}
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
			curEntry.recNum++;
		}
		writeBlock();
		writeDirEntry();
	}

	public static int numOfMetasPerBlock() {
		return (Block.SIZE - 8) / BlockMeta.SIZE;
	}

	/**
	 * TODO:metaBLockWriter文件应该也要以block的方式组织数据，这样便于扩展和管理
	 * 
	 * @throws IOException
	 */
	private void writeBlock() throws IOException {
		// write block level meta
		if (dataBlock.getRecs() != 0) {
			curBMeta.recNum = dataBlock.getRecs();
			curBMeta.write(memoryOutput);
			metaBlock.write(baos.toByteArray());
			baos.reset();
			blocks.add(dataBlock);

			curEntry.blockNum++;

			curBMeta.init();

			if ((curBlockIdx - 1) % (numOfMetasPerBlock() + 1) == 0) {
				flush();
			}
		}
	}

	public void close() throws IOException {
		flush();
		directoryWriter.close();
		idxIndexWriter.close();
	}
}
