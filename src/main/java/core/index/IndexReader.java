package core.index;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import segmentation.Interval;
import core.executor.PartitionExecutor;
import core.index.IndexWriter.DirEntry;

/**
 * 负责倒排索引的读取
 * 
 * @author xiafan
 * 
 */
public class IndexReader {
	private static final Logger logger = LoggerFactory
			.getLogger(IndexReader.class);
	
	// 记录每个keyword所在的partition
	HashMap<String, List<Integer>> partMeta = new HashMap<String, List<Integer>>();
	HashMap<Integer, IndexFileGroupReader> readerMeta = new HashMap<Integer, IndexFileGroupReader>();
	List<PartitionMeta> partitions = new ArrayList<PartitionMeta>();

	public void addPartitions(Path dir, Configuration conf) throws IOException {
		FileSystem fs = FileSystem.get(URI.create(dir.toString()), conf);
		FileStatus[] statuses = fs.listStatus(dir);
		for (FileStatus status : statuses) {
			if (status.isDir()) {
				String name = status.getPath().getName();
				name = name.substring("part".length());
				int partNum = Integer.parseInt(name);
				PartitionMeta meta = new PartitionMeta(partNum);
				addPartition(meta, status.getPath(), conf);
			}
		}
		logger.info("loading partitions is completed");
	}

	/**
	 * 将partition对应的dir下面所有的FileGroup加载到内存中去
	 * 
	 * @param dir
	 * @param conf
	 * @throws IOException
	 */
	public void addPartition(PartitionMeta meta, Path dir, Configuration conf)
			throws IOException {
		logger.info("loading partition " + meta.toString());
		partitions.add(meta);
		FileSystem fs = FileSystem.get(URI.create(dir.toString()), conf);

		FileStatus[] statuses = fs.listStatus(dir, new PathFilter() {
			public boolean accept(Path arg0) {
				if (arg0.getName().contains(".dir"))
					return true;
				return false;
			}
		});

		for (FileStatus status : statuses) {
			addPartFile(meta, status.getPath(), conf);
		}
	}

	/**
	 * 将file表示的partition加载到内存中去,
	 * 
	 * @param file
	 *            指向.dir后缀的文件
	 * @param conf
	 * @throws IOException
	 */
	public void addPartFile(PartitionMeta meta, Path file, Configuration conf)
			throws IOException {
		Path dir = file.getParent();
		String name = file.getName();
		int part = Integer.parseInt(name.substring(0, name.indexOf(".")));
		IndexFileGroupReader reader = new IndexFileGroupReader(meta);
		reader.open(dir, Integer.toString(part), conf);
		readerMeta.put(part, reader);
		Iterator<Entry<String, DirEntry>> iter = reader.iterDir();
		while (iter.hasNext()) {
			String keyword = iter.next().getKey();
			if (!partMeta.containsKey(keyword)) {
				partMeta.put(keyword, new ArrayList<Integer>());
			}
			partMeta.get(keyword).add(part);
		}
	}

	public List<PartitionMeta> getPartitions() {
		return partitions;
	}

	public List<PostingListCursor> cursor(String keyword, Interval window)
			throws IOException {
		List<PostingListCursor> ret = new ArrayList<PostingListCursor>();
		if (partMeta.containsKey(keyword)) {
			List<Integer> parts = partMeta.get(keyword);
			for (int part : parts) {
				ret.add(readerMeta.get(part).cursor(keyword, window));
			}
		}
		return ret;
	}

	public PostingListCursor cursor(String keyword, Interval window,
			PartitionMeta meta) throws IOException {
		PostingListCursor ret = null;
		if (partMeta.containsKey(keyword)) {
			List<Integer> parts = partMeta.get(keyword);
			for (int part : parts) {
				IndexFileGroupReader reader = readerMeta.get(part);
				if (reader.getMeta().equals(meta)) {
					ret = reader.cursor(keyword, window);
					break;
				}
			}
		}
		return ret;
	}

	public Iterator<String> keywordsIter() {
		return partMeta.keySet().iterator();
	}

	public void close() throws IOException {
		partMeta.clear();
		for (IndexFileGroupReader reader : readerMeta.values()) {
			reader.close();
		}
	}

	public static void main(String args[]) {
		Path dir = new Path("D:\\RWork\\微博查询\\index");
		Configuration conf = null;
		/**
		 * 创建
		 */

		IndexReader indexReader = new IndexReader();
		try {
			indexReader.addPartition(new PartitionMeta(16), dir, conf);
			PartitionExecutor executor = new PartitionExecutor(indexReader);
			executor.setMaxLifeTime(Integer.MAX_VALUE);
			String keyword[] = new String[] { "大数据", "机器学习" };
			Interval window = new Interval(1, 1, 1, 1);
			TempKeywordQuery query = new TempKeywordQuery(keyword, window, 10);
			executor.query(query);
			Iterator<Interval> res = executor.getAnswer();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
