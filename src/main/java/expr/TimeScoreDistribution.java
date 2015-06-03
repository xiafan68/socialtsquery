package expr;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.PriorityQueue;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import segmentation.Interval;
import xiafan.file.FileUtil;
import xiafan.util.Pair;
import core.index.IndexReader;
import core.index.MidSegment;
import core.index.PartitionMeta;
import core.index.PostingListCursor;

public class TimeScoreDistribution {
	IndexReader indexReader;
	PriorityQueue<PostingListCursor> queue;

	public void loadIndex(Path dir, boolean multiPart) throws IOException {
		indexReader = new IndexReader();
		if (multiPart) {
			indexReader.addPartitions(dir, new Configuration());
		} else {
			indexReader.addPartition(new PartitionMeta(17), dir,
					new Configuration());
		}
	}

	private void loadCursors(String keyword) throws IOException {
		List<PostingListCursor> cursors = indexReader.cursor(keyword,
				new Interval(1, 0, 100000000, 1));
		queue = new PriorityQueue<PostingListCursor>(cursors.size(),
				new Comparator<PostingListCursor>() {
					@Override
					public int compare(PostingListCursor arg0,
							PostingListCursor arg1) {
						return arg0.peek().compareTo(arg1.peek());
					}
				});
		queue.addAll(cursors);
	}

	private void dump(String keyword, String file) throws IOException {
		loadCursors(keyword);
		DataOutputStream output = FileUtil.openDos(file);
		Map<Pair<Integer, Integer>, Integer> map = new TreeMap<Pair<Integer, Integer>, Integer>(
				new Comparator<Pair<Integer, Integer>>() {
					@Override
					public int compare(Pair<Integer, Integer> arg0,
							Pair<Integer, Integer> arg1) {
						int ret = arg0.arg0.compareTo(arg1.arg0);
						if (ret == 0) {
							ret = arg0.arg1.compareTo(arg1.arg1);
						}
						return ret;
					}
				});
		while (!queue.isEmpty()) {
			PostingListCursor cur = queue.poll();
			MidSegment seg = cur.next();
			Pair<Integer, Integer> pair = new Pair<Integer, Integer>(Math.max(
					seg.getStartCount(), seg.getEndCount()), seg.getStart());
			if (!map.containsKey(pair)) {
				map.put(pair, 1);
			} else {
				map.put(pair, map.get(pair) + 1);
			}
			if (cur.hasNext())
				queue.offer(cur);

		}
		for (Entry<Pair<Integer, Integer>, Integer> entry : map.entrySet()) {
			output.write(String.format("%d\t%d\t%d\n", entry.getKey().arg0,
					entry.getKey().arg1, entry.getValue()).getBytes());
		}
		output.close();
	}

	public static void main(String[] args) throws IOException {
		TimeScoreDistribution tsd = new TimeScoreDistribution();
		tsd.loadIndex(new Path(args[0]), Boolean.parseBoolean(args[1]));
		tsd.dump("奥运会", "/home/xiafan/KuaiPan/dataset/aoyun.txt");
	}
}
