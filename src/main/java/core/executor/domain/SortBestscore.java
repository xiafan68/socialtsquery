package core.executor.domain;

import java.util.Comparator;

/**
 * 从大到小的排序函数
 * 
 * @author dingcheng
 * @version 0.1 2015/2/9.
 */
public enum SortBestscore implements Comparator<MergedMidSeg> {
	INSTANCE;

	/**
	 * 降序
	 */
	public int compare(MergedMidSeg o1, MergedMidSeg o2) {
		int ret = o1.getBestscore() > o2.getBestscore() ? -1 : o1.getBestscore() == o2.getBestscore() ? 0 : 1;
		if (ret == 0) {
			ret = Long.compare(o1.getMid(), o2.getMid());
		}
		return ret;
	}
}
