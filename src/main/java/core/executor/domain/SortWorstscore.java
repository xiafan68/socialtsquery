package core.executor.domain;

import java.util.Comparator;

/**
 * 用于小跟堆的比较
 * @author dingcheng
 * @version 0.1 2015/2/9.
 */
public enum SortWorstscore implements Comparator<MergedMidSeg> {
	INSTANCE;
	public int compare(MergedMidSeg o1, MergedMidSeg o2) {
		int ret = o1.getWorstscore() > o2.getWorstscore() ? 0 : o1
				.getWorstscore() == o2.getWorstscore() ? 0 : 1;
		if (ret == 0) {
			ret = Long.compare(o1.getMid(), o2.getMid());
		}
		return ret;
	}
}
