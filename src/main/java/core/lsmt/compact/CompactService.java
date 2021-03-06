package core.lsmt.compact;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentSkipListSet;

import org.apache.log4j.Logger;

import core.lsmo.bdbformat.OctreeSSTableWriter;
import core.lsmt.IMemTable.SSTableMeta;
import core.lsmt.ISSTableReader;
import core.lsmt.ISSTableWriter;
import core.lsmt.LSMTInvertedIndex;
import core.lsmt.LSMTInvertedIndex.VersionSet;
import io.FileUtil;
import util.Configuration;

/**
 * to avoid scan disk to find those posting lists needing compaction, a lazy
 * policy is adopted. Only posting list that has come into memory will be
 * checked for compaction
 * 
 * @author xiafan
 * 
 */
public class CompactService extends Thread {
	private static final Logger logger = Logger.getLogger(CompactService.class);
	LSMTInvertedIndex index;
	ConcurrentSkipListSet<String> compactingWords = new ConcurrentSkipListSet<String>();
	VersionSet snapshot;
	ICompactStrategy iCompactStrategy;

	public CompactService(LSMTInvertedIndex index) {
		super("compaction thread");
		this.index = index;
		iCompactStrategy = index.getConf().getCompactStragety();
	}

	/**
	 * TODO
	 * 
	 * @return
	 */
	private List<SSTableMeta> fileToCompact() {
		List<SSTableMeta> ret = new ArrayList<SSTableMeta>();
		snapshot = index.getVersion();
		TreeMap<Integer, Integer> levelNums = new TreeMap<Integer, Integer>();
		Map<Integer, List> levelList = new HashMap<Integer, List>();

		for (SSTableMeta meta : snapshot.diskTreeMetas) {
			if (levelNums.containsKey(meta.level)) {
				levelList.get(meta.level).add(meta);
				levelNums.put(meta.level, levelNums.get(meta.level) + 1);
			} else {
				levelNums.put(meta.level, 1);
				ArrayList<SSTableMeta> metas = new ArrayList<SSTableMeta>();
				metas.add(meta);
				levelList.put(meta.level, metas);
			}
		}
		ArrayList<Entry<Integer, Integer>> sortedLevelNum = new ArrayList<Entry<Integer, Integer>>(
				levelNums.entrySet());
		if (sortedLevelNum.isEmpty()) {
			return ret;
		}

		Collections.sort(sortedLevelNum, new Comparator<Entry<Integer, Integer>>() {

			@Override
			public int compare(Entry<Integer, Integer> o1, Entry<Integer, Integer> o2) {
				return o2.getValue().compareTo(o1.getValue());
			}
		});
		ret = levelList.get(sortedLevelNum.get(0).getKey());
		return ret;
	}

	@Override
	public void run() {
		while (!index.stop || index.stopOnWait) {
			try {
				snapshot = index.getVersion();
				List<SSTableMeta> toCompact = iCompactStrategy.compactFiles(snapshot.diskTreeMetas);
				if (index.stop && (!index.stopOnWait || toCompact.size() < 2)) {
					break;
				}
				if (!compactTrees(toCompact)) {
					try {
						Thread.sleep(1000);
					} catch (InterruptedException e) {
					}
				}
			} catch (Exception ex) {
				logger.error("compact service:" + ex.getMessage());
				index.stop = true;
				StringWriter swriter = new StringWriter();
				PrintWriter pwriter = new PrintWriter(swriter);
				ex.printStackTrace(pwriter);
				logger.error(swriter.toString());
				throw new RuntimeException(ex.getMessage());
			}
		}
	}

	public boolean compactTrees(List<SSTableMeta> toCompact) {
		// find out trees needing compaction
		// TODO 1. 确定需要压缩的文件。2. 获取相关的SSTableReader， 3. 使用SSTableWriter写出数据 4.
		// 使用Index更新版本集合。
		boolean ret = false;
		if (toCompact.size() >= 2) {
			StringBuffer buf = new StringBuffer("compacting versions ");
			toCompact = toCompact.subList(0, 2);
			for (SSTableMeta meta : toCompact) {
				buf.append(meta);
				buf.append(" ");
			}
			logger.info(buf.toString());

			ret = true;

			List<ISSTableReader> readers = new ArrayList<ISSTableReader>();
			for (SSTableMeta meta : toCompact) {
				if (!meta.markAsDel.get()) {
					try {
						readers.add(index.getSSTableReader(snapshot, meta));
					} catch (Exception ex) {
						StringWriter sw = new StringWriter();
						PrintWriter printStream = new PrintWriter(sw);
						ex.printStackTrace(printStream);
						logger.error(sw.toString());
					}
				}
			}

			int newLevel = Math.max(toCompact.get(0).level, toCompact.get(1).level) + 1;
			int newVersion = Math.max(toCompact.get(0).version, toCompact.get(1).version);
			SSTableMeta newMeta = new SSTableMeta(newVersion, newLevel);
			ISSTableWriter writer = index.getLSMTFactory().newSSTableWriterForCompaction(newMeta, readers,
					index.getConf());
			try {
				Configuration conf = index.getConf();
				writer.open(conf.getTmpDir());
				writer.write();
				writer.close();

				writer.moveToDir(conf.getTmpDir(), conf.getIndexDir());
				index.compactTables(new HashSet<SSTableMeta>(toCompact), writer.getMeta());
			} catch (IOException e) {
				StringWriter sw = new StringWriter();
				PrintWriter printStream = new PrintWriter(sw);
				e.printStackTrace(printStream);
				logger.error(sw.toString());
				throw new RuntimeException(e);
			}
		}
		snapshot = null;
		return ret;
	}

	private void markAsDel(List<SSTableMeta> toCompact) throws IOException {
		for (SSTableMeta meta : toCompact) {
			File file = OctreeSSTableWriter.dataFile(index.getConf().getIndexDir(), meta);
			FileUtil.markDel(file);
		}
	}
}
