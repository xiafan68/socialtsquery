package expr;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

import segmentation.Interval;
import xiafan.file.DirLineReader;

public class TQueryGen extends QueryGen {
	public TQueryGen(int i) {
		super(i);
	}

	List<TQuery> queries = new LinkedList<TQuery>();

	public void loadTQuery(String file) throws IOException {
		DirLineReader reader = new DirLineReader(file);
		String line = null;
		while (null != (line = reader.readLine())) {
			TQuery query = new TQuery();
			query.parse(line);
			queries.add(query);
		}
		reader.close();
	}

	List<Integer> numbers = new ArrayList<Integer>();

	private void loadQueryWithNum(String seedFile) throws IOException {
		DirLineReader reader = new DirLineReader(seedFile);
		String line = null;
		int count = 0;
		while (null != (line = reader.readLine())) {
			String[] fields = line.split("\t");
			List<String> keywords = new ArrayList<String>();
			keywords.add(fields[0]);
			keywords.add(fields[1]);
			if (keywords.equals(lists.get(count))) {
				numbers.add(Integer.parseInt(fields[2]));
				count++;
			}
			if (count >= lists.size())
				break;
		}
		reader.close();
	}

	@Override
	public boolean hasNext() {
		return cur < queries.size();
	}

	/**
	 * return query in a round robin manner
	 * 
	 * @return
	 */
	public TQuery nextTQuery() {
		int idx = cur++ % queries.size();
		return queries.get(idx);
	}

	public static class TQuery {
		public List<String> keywords;
		public int k;
		public Interval tQuery;

		public void parse(String line) {
			String[] fields = line.split("\t");
			k = Integer.parseInt(fields[0]);
			tQuery = new Interval(0, Integer.parseInt(fields[1]),
					Integer.parseInt(fields[2]), 1f);
			keywords = new ArrayList<String>();
			keywords.add(fields[3]);
			keywords.add(fields[4]);
		}

		@Override
		public String toString() {
			return "TQuery [keywords=" + keywords + ", k=" + k + ", tQuery="
					+ tQuery + "]";
		}

	}

	private static class ElementGen extends ProbIndexGen {
		int[] data;

		public ElementGen(int[] data, double[] probs) {
			super(probs);
			this.data = data;
		}

		@Override
		public int next() {
			return data[super.next()];
		}
	}

	private static class ProbIndexGen {
		double[] accProb;

		public ProbIndexGen(double[] probs) {
			accProb = new double[probs.length - 1];
			float sum = 0.0f;
			for (int i = 0; i < probs.length - 1; i++) {
				sum += probs[i];
				accProb[i] = sum;
			}
		}

		Random rand = new Random();

		public int next() {
			double prob = rand.nextDouble();
			int idx = Arrays.binarySearch(accProb, prob);
			if (idx < 0) {
				return Math.abs(idx) - 1;
			}
			return idx;
		}
	}

}
