package core.index;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;

import segmentation.Interval;
import Util.Profile;
import core.index.IndexWriter.BlockMeta;
import core.index.IndexWriter.DirEntry;

/**
 * 倒排记录Cursor
 */
public class PostingListCursor implements Iterator<MidSegment> {
	DirEntry entry;
	IndexFileGroupReader reader;
	Interval window;

	int postingReadRecs = 0;// 当前读到第几个MidSegment
	int blockReadedRecs = 0;// 记录读到当前block的第几个MidSegment
	int readedBlockNum = 0;
	int curDataBlockIdx = 0;

	Block curBlock = null;
	BlockMeta curBlockMeta = null;
	Queue<BlockMeta> bMetas = new LinkedList<BlockMeta>();
	DataInputStream curInpuStream;
	MidSegment cur = null;

	public PostingListCursor(IndexFileGroupReader reader, DirEntry entry,
			Interval window) {
		this.reader = reader;
		this.entry = entry;
		this.curDataBlockIdx = entry.dataBlockIdx;
		this.window = window;
	}

	/**
	 * 打开一条倒排索引
	 */
	public void open() throws IOException {
		loadNextBlock();
	}

	/**
	 * 跳到一个包含有效数据的block
	 * 
	 * @throws IOException
	 */
	private void locateDataBlock() throws IOException {
		boolean found = false;
		while (readedBlockNum < entry.blockNum && !found) {
			if (bMetas.isEmpty()) {
				loadBMetas();
			}

			while (!bMetas.isEmpty() && readedBlockNum < entry.blockNum) {
				curBlockMeta = bMetas.poll();
				if (curBlockMeta.startTime > window.getEnd()
						|| curBlockMeta.endTime < window.getStart()) {
					postingReadRecs += curBlockMeta.recNum;
					readedBlockNum++;
					curDataBlockIdx = IndexWriter
							.nextDataBlockIdx(curDataBlockIdx);
				} else {
					found = true;
					break;
				}
			}
		}
		if (!found) {
			curDataBlockIdx = -1;
		}
	}

	/**
	 * 加载从当前待读取的data block的meta block
	 * 
	 * @throws IOException
	 */
	private void loadBMetas() throws IOException {
		int metaBlockIdx = IndexWriter.bMetaBlockIdx(curDataBlockIdx);
		Block block = reader.loadBlock(metaBlockIdx);
		assert block.getType() == Block.META_BLOCK;

		int idx = (curDataBlockIdx - metaBlockIdx) - 1;
		DataInputStream dis = block.readByStream();
		for (int i = 0; i < block.getRecs(); i++) {
			BlockMeta meta = new BlockMeta(0, 0, 0);
			meta.read(dis);
			if (i < idx) {
				continue;
			}
			bMetas.add(meta);
		}
	}

	private void loadNextBlock() throws IOException {
		locateDataBlock();
		Profile.instance.updateCounter(Profile.ATOMIC_IO);
		Profile.instance.updateCounter(Profile.ATOMIC_IO
				+ reader.getMeta().partIdx);
		Profile.instance.start(Profile.toTimeTag(Profile.ATOMIC_IO));
		curBlock = new Block(Block.DATA_BLOCK);
		reader.loadBlock(curBlock, curDataBlockIdx);
		Profile.instance.end(Profile.toTimeTag(Profile.ATOMIC_IO));

		assert curBlock.getRecs() == curBlockMeta.recNum;

		readedBlockNum++;
		curDataBlockIdx = IndexWriter.nextDataBlockIdx(curDataBlockIdx);
		curInpuStream = curBlock.readByStream();
		blockReadedRecs = 0;
		Profile.instance.updateCounter(entry.keyword);
	}

	// 是否读完该条倒排索引。
	public boolean hasNext() {
		if (cur == null)
			advance();
		return cur != null;
	}

	private void advance() {
		MidSegment seg = new MidSegment();
		while (postingReadRecs < entry.recNum && cur == null) {
			try {
				if (blockReadedRecs++ == curBlock.recs) {
					loadNextBlock();
					// loadNextBlock重置了blockIdx，这里需要重新加1!!!!!
					// blockReadedRecs++;
					continue;
				}
				postingReadRecs++;
				seg.readFields(curInpuStream);
				// TODO 判断seg是否和window相交
				if (seg.getStart() <= window.getEnd()
						&& seg.getEndTime() >= window.getStart()) {
					// 相交，查找成功。
					cur = seg;
					break;
				} else {
					Profile.instance.updateCounter(Profile.TWASTED_REC);
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * @return
	 */
	public float getBestScore() {
		if (cur == null)
			advance();

		if (cur != null)
			return Math.max(cur.getStartCount(), cur.getEndCount());
		else {
			return 0.0f;
		}
	}

	// 读下一条记录。
	public MidSegment next() {
		if (cur == null)
			advance();
		MidSegment ret = cur;
		cur = null;
		return ret;
	}

	public void remove() {
		throw new RuntimeException("not implemented");
	}

	public void close() throws IOException {

	}
}
