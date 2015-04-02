package core.index;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

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

	int postingIdx = 0;// 当前读到第几个MidSegment
	int blockIdx = 0;
	long curOffset = 0;
	
	Block curBlock = new Block();
	List<BlockMeta> bMetas = new ArrayList<BlockMeta>();
	DataInputStream curInpuStream;
	MidSegment cur = null;

	public PostingListCursor(IndexFileGroupReader reader, DirEntry entry,
			Interval window) {
		this.reader = reader;
		this.entry = entry;
		this.curOffset = entry.indexPos;
		this.window = window;
	}

	/**
	 * 打开一条倒排索引
	 */
	public void open() throws IOException {
		loadNextBlock();
	}

	private void loadNextBlock() throws IOException {
		Profile.instance.updateCounter(Profile.ATOMIC_IO);
		Profile.instance.updateCounter(Profile.ATOMIC_IO+ reader.getMeta().partIdx);
		Profile.instance.start(Profile.toTimeTag(Profile.ATOMIC_IO));
		reader.loadBlock(curBlock, curOffset);
		Profile.instance.end(Profile.toTimeTag(Profile.ATOMIC_IO));
		curOffset += Block.SIZE;
		curInpuStream = curBlock.readByStream();
		blockIdx = 0;
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
		while (postingIdx++ < entry.size && cur == null) {
			try {
				if (blockIdx++ == curBlock.recs) {
					loadNextBlock();
					// loadNextBlock重置了blockIdx，这里需要重新加1!!!!!
					blockIdx++;
				}
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
