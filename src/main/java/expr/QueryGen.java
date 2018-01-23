package expr;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import io.DirLineReader;
import util.Pair;

/**
 * 基于文件生成temporal keyword search
 * 
 * @author xiafan
 * 
 */
public class QueryGen {
	List<List<String>> lists = new ArrayList<List<String>>();
	List<Integer> starts = new ArrayList<Integer>();

	int cur = 0;
	int limit = Integer.MAX_VALUE;

	public QueryGen(int i) {
		limit = i;
	}

	/**
	 * 加载文件
	 * 
	 * @throws IOException
	 */
	public void loadQueryWithTime(String seedFile) throws IOException {
		// "/home/xiafan/KuaiPan/dataset/time_series/nqueryseed.txt"
		DirLineReader reader = new DirLineReader(seedFile);
		String line = null;
		int count = 0;

		while (null != (line = reader.readLine())) {
			// if (count++ < 4)
			// continue;

			String[] fields = line.split("\t");
			List<String> keywords = new ArrayList<String>();
			int keywordNum = Integer.parseInt(fields[0]);
			for (int i = 1; i < keywordNum+1; i++)
				keywords.add(fields[i]);
			lists.add(keywords);
			starts.add(Integer.parseInt(fields[keywordNum + 1]));
			if (count++ > limit)
				break;
			if (count % 1000 == 0)
				System.out.println(count);
		}
		reader.close();
	}

	public Pair<List<String>, Integer> nextQuery() {
		int idx = cur++ % lists.size();
		return new Pair<List<String>, Integer>(lists.get(idx), starts.get(idx));
	}

	public boolean hasNext() {
		if (cur < lists.size())
			return true;
		return false;
	}

	public int size() {
		return lists.size();
	}

	public void resetCur() {
		cur = 0;
	}
}
