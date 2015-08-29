package core.lsmt;

public class PostingListMeta {
	public int size = 0;
	public int maxTime = Integer.MIN_VALUE;
	public int minTime = Integer.MAX_VALUE;

	public PostingListMeta() {

	}

	public PostingListMeta(PostingListMeta meta) {
		size = meta.size;
		minTime = meta.minTime;
		maxTime = meta.maxTime;
	}

	public PostingListMeta(PostingListMeta a, PostingListMeta b) {
		size = a.size + b.size;
		minTime = Math.min(a.minTime, b.minTime);
		maxTime = Math.max(a.maxTime, b.maxTime);
	}

	public void merge(PostingListMeta o) {
		size += o.size;
		minTime = Math.min(minTime, o.minTime);
		maxTime = Math.max(maxTime, o.maxTime);
	}
}
