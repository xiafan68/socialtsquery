package expr;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map.Entry;
import java.util.Random;

import expr.InsertStreamGen.Subscriber;
import expr.WorkLoadGen.WorkLoad;
import searchapi.QueryType;
import searchapi.TKeywordsQuery;
import util.Configuration;
import util.Pair;

/**
 * 生成实时查询与历史查询混合的查询流
 * 
 * @author xiafan
 *
 */
public class ReadStreamGen implements Subscriber {
	volatile List<Entry<String, Integer>> popWords;
	volatile long lastTimeStamp;
	QueryGen gen;
	Random toss = new Random();
	float[] fracs;
	private Configuration conf;

	public ReadStreamGen(float[] fracs) {
		this.fracs = fracs;
	}

	public void init(String seedFile, String confFile) throws IOException {
		gen = new QueryGen(100);
		gen.loadQueryWithTime(seedFile);

		conf = new Configuration();
		conf.load(confFile);
	}

	@Override
	public void onPopWordsChanged(List<Entry<String, Integer>> popWords, long time) {
		this.popWords = popWords;
		this.lastTimeStamp = time;
	}

	public WorkLoad genNextJob() {
		float coin = toss.nextFloat();
		WorkLoad ret = null;
		while (ret != null) {
			if (coin < fracs[0]) {
				// realtime query
				String word = null;
				if (popWords != null) {
					List<Entry<String, Integer>> tmpWords = popWords;
					word = tmpWords.get(Math.abs(toss.nextInt() % tmpWords.size())).getKey();
				}
				if (word != null) {
					int start = (int) lastTimeStamp;
					ret = new WorkLoad(WorkLoad.QUEYR_JOB, new TKeywordsQuery(Arrays.asList(word), 50,
							start - conf.queryStartTime(), start - conf.queryStartTime() + 12, QueryType.WEIGHTED));
				}
			} else {
				// historical query
				Pair<List<String>, Integer> seed = gen.nextQuery();
				ret = new WorkLoad(WorkLoad.QUEYR_JOB,
						new TKeywordsQuery(seed.getKey(), 50, seed.getValue() - conf.queryStartTime(),
								seed.getValue() - conf.queryStartTime() + 24 * 2 * 30, QueryType.WEIGHTED));
			}
		}
		return ret;
	}

}
