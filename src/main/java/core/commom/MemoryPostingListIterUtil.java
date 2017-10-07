package core.commom;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import common.MidSegment;
import core.lsmt.postinglist.IPostingListIterator;
import core.lsmt.postinglist.PostingListMeta;
import util.Pair;

public class MemoryPostingListIterUtil {

	/**
	 * 直接将内存中的倒排表中所有符合条件的数据选出来，作为一个iter返回
	 * 
	 * @param iter
	 * @param start
	 * @param end
	 * @return
	 * @throws IOException
	 */
	public static IPostingListIterator getPostingListIter(IPostingListIterator iter, int start, int end) {
		final List<MidSegment> list = new ArrayList<MidSegment>();
		int max = 0;
		try {
			while (iter.hasNext()) {
				Pair<Integer, List<MidSegment>> pair = iter.next();
				max = Math.max(max, pair.getKey());
				for (MidSegment mid : pair.getValue()) {
					if (mid.getStart() <= end && mid.getEndTime() >= start) {
						list.add(mid);
					}
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		final int threshold = max;
		return new IPostingListIterator() {
			boolean hasNext = true;

			@Override
			public PostingListMeta getMeta() {
				return null;
			}

			@Override
			public void open() throws IOException {

			}

			@Override
			public boolean hasNext() throws IOException {
				return hasNext;
			}

			@Override
			public Pair<Integer, List<MidSegment>> next() throws IOException {
				hasNext = false;
				return new Pair<Integer, List<MidSegment>>(threshold, list);
			}

			@Override
			public void skipTo(WritableComparableKey key) throws IOException {

			}

			@Override
			public void close() throws IOException {

			}
		};
	}
}
