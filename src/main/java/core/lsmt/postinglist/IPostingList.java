package core.lsmt.postinglist;

import common.MidSegment;

public abstract class IPostingList {
	protected PostingListMeta meta;

	public IPostingList(PostingListMeta meta) {
		this.meta = meta;
	}

	public PostingListMeta getMeta() {
		return meta;
	}

	public int size() {
		return meta.size;
	}

	public abstract boolean insert(MidSegment seg);
}
