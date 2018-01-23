package core.commom;

import java.util.Arrays;

import segmentation.Interval;

/**
 * temporal keyword query类
 * 
 * @author xiafan
 * 
 */
public class TempKeywordQuery {
	public String[] keywords;
	public Interval queryInv;
	public int k;

	public TempKeywordQuery(String[] keywords, Interval queryInv, int k) {
		super();
		this.keywords = keywords;
		this.queryInv = queryInv;
		this.k = k;
	}

	public int getStartTime() {
		return queryInv.getStart();
	}

	public int getEndTime() {
		return queryInv.getEnd();
	}

	public int getQueryWidth() {
		return queryInv.getEnd() - queryInv.getStart();
	}

	@Override
	public String toString() {
		return "TempKeywordQuery [keywords=" + Arrays.toString(keywords)
				+ ", queryInv=" + queryInv + ", k=" + k + "]";
	}
	
}
