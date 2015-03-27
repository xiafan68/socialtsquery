package core.executor.domain;

import java.util.Comparator;

/**
 * 用于priorityqueue时，queue是大根堆
 * 
 * @author dingcheng
 * @version 0.1 2015/2/9.
 */
public class SortBestscore implements Comparator<MergedMidSeg> {
	public SortBestscore() {

	}

	/**
	 * 降序
	 */
	public int compare(MergedMidSeg o1, MergedMidSeg o2) {
		int ret = o1.getBestscore() > o2.getBestscore() ? -1 : o1
				.getBestscore() == o2.getBestscore() ? 0 : 1;
		if (ret == 0) {
			ret = Long.compare(o1.getMid(), o2.getMid());
		}
		return ret;
	}
}
