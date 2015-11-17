package core.lsmt;

import java.io.IOException;
import java.util.List;

import common.MidSegment;
import util.Pair;

/**
 * 用于遍历当前postinglist的接口类
 * @author xiafan
 *
 */
public interface IPostingListIterator {
	public PostingListMeta getMeta();

	public void open() throws IOException;

	public boolean hasNext() throws IOException;

	/**
	 * pair.arg0表示的是当前postinglist的最大值
	 * pair.arg1表示的是当前返回的一组MidSegment
	 * 借口只能保证按照pair.arg0的非递增序返回
	 * @return
	 * @throws IOException
	 */
	public Pair<Integer, List<MidSegment>> next() throws IOException;

	public void skipTo(WritableComparableKey key) throws IOException;

	public void close() throws IOException;

}
