package core.index;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import segmentation.Interval;
import core.index.IndexWriter.BlockMeta;
import core.index.IndexWriter.DirEntry;
import core.io.LinuxSeekableDirectIO;
import core.io.SeekableDirectIO;

/**
 * 负责读取索引的一个FileGroup的读取，一个FileGroup包括.idx,.dir和.bmeta
 * 
 * @author: dingcheng
 * @author xiafan
 */
public class IndexFileGroupReader {
	PartitionMeta meta;
	Path dir;
	String part;
	Configuration conf;
	// RandomAccessFile idxInput;
	SeekableDirectIO idxInput;
	SeekableDirectIO bMetaInput;
	/*
	 * 每个词在索引文件中对应的位置。
	 */
	protected HashMap<String, IndexWriter.DirEntry> dirMap = null;

	public IndexFileGroupReader(PartitionMeta meta) {
		super();
		this.meta = meta;
	}

	/**
	 * 加载directory项到内存中
	 * 
	 * @param dir
	 * @param part
	 * @param conf
	 */
	public void open(Path dir, String part, Configuration conf)
			throws IOException {
		this.dir = dir;
		this.part = part;
		this.conf = conf;
		openDir();
	}

	/**
	 *
	 */
	protected void openDir() throws IOException {
		dirMap = new HashMap<String, IndexWriter.DirEntry>();
		FileSystem fs = FileSystem.get(URI.create(dir.toString()), conf);
		Path path = new Path(dir, part + "." + IndexWriter.DIR_SUFFIX);
		DataInputStream dis = new DataInputStream(new BufferedInputStream(
				fs.open(path)));

		while (dis.available() > 0) {
			IndexWriter.DirEntry entry = new IndexWriter.DirEntry();
			entry.read(dis);
			dirMap.put(entry.keyword, entry);
			// System.out.println(entry);
		}
		// System.out.println(i);
		dis.close();

		path = new Path(dir, part + "." + IndexWriter.IDX_SUFFIX);
		// RandomAccessFile randomAccessFile = new
		// RandomAccessFile(path.toUri().getPath(), "r");
		// idxInput = randomAccessFile;
		idxInput = new LinuxSeekableDirectIO(path.toUri().getPath());
		path = new Path(dir, part + "." + IndexWriter.BMETA_SUFFIX);
		bMetaInput = new LinuxSeekableDirectIO(path.toUri().getPath());
	}

	/**
	 * 给定关键词，找到在索引文件中的DirEntry(off和len)
	 * 
	 * @param keyword
	 * @return
	 */
	public IndexWriter.DirEntry getDirEntry(String keyword) {
		return dirMap.get(keyword);
	}

	/**
	 * 获得一个结果集。查询，建立两个堆，停止查询的条件。
	 * 
	 * @throws IOException
	 */
	public PostingListCursor cursor(String keyword, Interval window)
			throws IOException {
		// TODO 通过keyword和window去索引里查询，
		IndexWriter.DirEntry dirEntry = getDirEntry(keyword);
		if (dirEntry == null)
			return null;
		PostingListCursor ret = new PostingListCursor(this, dirEntry, window);
		ret.open();
		return ret;
	}

	public Block loadBlock(long offset) throws IOException {
		Block ret = new Block();
		loadBlock(ret, offset);
		return ret;
	}

	public void loadBlock(Block block, long offset) throws IOException {
		// idxInput.getChannel().position(offset);
		idxInput.position(offset);
		idxInput.read(block.getBytes());
		block.init();
	}

	public BlockMeta loadBMeta(long offset) throws IOException {
		bMetaInput.position(offset);
		byte[] data = new byte[12];
		bMetaInput.read(data);
		BlockMeta ret = new BlockMeta(0, 0, 0);
		ret.read(new DataInputStream(new ByteArrayInputStream(data)));
		return ret;
	}

	public Iterator<Entry<String, DirEntry>> iterDir() {
		return dirMap.entrySet().iterator();
	}

	/**
	 * @return the meta
	 */
	public PartitionMeta getMeta() {
		return meta;
	}

	public void close() throws IOException {
		idxInput.close();
		bMetaInput.close();
	}

	//
	// public void testOne() {
	// Long offset = 0;
	// Long length = 100;
	// String word = "";
	// Interval window = new Interval(100000000, 0, 100, 1);
	//
	// PostingListCursor plc = new PostingListCursor(offset, length, word,
	// window);
	//
	// MidSegment midseg = plc.next();
	// if (midseg != null) {
	// System.out.println(midseg.getStart());
	// }else {
	// System.out.println("*********************");
	// }
	// }

}
