package expr;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Comparator;
import java.util.Iterator;
import java.util.PriorityQueue;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import xiafan.file.FileUtil;
import xiafan.util.Pair;
import core.index.IndexReader;
import core.index.IndexWriter.DirEntry;
import core.index.PartitionMeta;
import core.index.PostingListCursor;

public class SortKeywordByFreq {
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

	public void dump(String path) throws IOException {
		TreeSet<Pair<String, Integer>> keys = new TreeSet<Pair<String, Integer>>(
				new Comparator<Pair<String, Integer>>() {

					@Override
					public int compare(Pair<String, Integer> arg0,
							Pair<String, Integer> arg1) {
						int ret = 0 - Integer.compare(arg0.arg1, arg1.arg1);
						if (ret == 0) {
							ret = arg0.arg0.compareTo(arg1.arg0);
						}
						return ret;
					}
				});
		DataOutputStream output = FileUtil.openDos(path);
		Iterator<String> iter = indexReader.keywordsIter();
		while (iter.hasNext()) {
			String curKey = iter.next();
			int count = 0;
			for (DirEntry entry : indexReader.getEntries(curKey)) {
				count += entry.recNum;
			}
			keys.add(new Pair<String, Integer>(curKey, count));
		}

		for (Pair<String, Integer> data : keys) {
			output.write(String.format("%s\t%d\n", data.arg0, data.arg1)
					.getBytes());
		}
		output.close();
	}

	/**
	 * args[0]:索引目录 args[1]:输出目录
	 * 
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		SortKeywordByFreq sorter = new SortKeywordByFreq();
		sorter.loadIndex(new Path(args[0]), true);
		sorter.dump(args[1]);
	}
}
