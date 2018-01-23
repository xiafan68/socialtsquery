package core.executor;

import java.util.Arrays;

import core.commom.TempKeywordQuery;

/**
 * 为 @see core.index.domain.MergedMidSeg 类提供计算分值所需要的信息
 * 
 * @author xiafan
 * 
 */
public class ExecContext {
	TempKeywordQuery query;
	int[] lifeTimeBound;
	float[] bestScores; // 每个posting list的最好值
	float[] weights; // 对应于每个关键词的权重,可能是用户提供的
	
	public ExecContext(TempKeywordQuery query) {
		this.query = query;
		this.weights = new float[query.keywords.length];
		//TODO 目前每个词的权重都一样
		Arrays.fill(weights, 1.0f);
	}
	

	public void setQuery(TempKeywordQuery query) {
		this.query = query;
	}

	public void setLifeTimeBound(int[] lifeTimeBound) {
		this.lifeTimeBound = lifeTimeBound;
	}

	public void setBestScores(float[] bestScores) {
		this.bestScores = bestScores;
	}

	public void setWeights(float[] weights) {
		this.weights = weights;
	}

	public TempKeywordQuery getQuery() {
		return query;
	}

	public int[] getLifeTimeBound() {
		return lifeTimeBound;
	}

	public TempKeywordQuery getTempKeywordQuery() {
		return query;
	}

	public float getBestScore(int idx) {
		return bestScores[idx];
	}

	public float getWeight(int idx) {
		return weights[idx];
	}
}
