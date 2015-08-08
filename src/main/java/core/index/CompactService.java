package core.index;

import java.io.File;
import java.io.IOException;
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

import Util.Configuration;
import core.index.LSMOInvertedIndex.VersionSet;
import core.index.MemTable.SSTableMeta;
import fanxia.file.FileUtil;

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
	LSMOInvertedIndex index;
	ConcurrentSkipListSet<String> compactingWords = new ConcurrentSkipListSet<String>();
	VersionSet snapshot;

	public CompactService(LSMOInvertedIndex index) {
		super("compaction thread");
		this.index = index;
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
		while (!index.stop) {
			if (!compactTrees()) {
				try {
					Thread.sleep(1000);
					// logger.info("compaction thread wakes up");
				} catch (InterruptedException e) {
				}
			}
		}
	}

	public boolean compactTrees() {
		// find out trees needing compaction
		// TODO 1. 确定需要压缩的文件。2. 获取相关的SSTableReader， 3. 使用SSTableWriter写出数据 4.
		// 使用Index更新版本集合。
		boolean ret = false;
		List<SSTableMeta> toCompact = fileToCompact();
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
				try {
					readers.add(index.getSSTableReader(snapshot, meta));
				} catch (Exception ex) {
					logger.error(ex.getStackTrace());
				}
			}

			SSTableMeta newMeta = new SSTableMeta(toCompact.get(toCompact.size() - 1).version,
					toCompact.get(0).level + 1);
			SSTableWriter writer = new SSTableWriter(newMeta, readers, index.getStep());
			try {
				Configuration conf = index.getConf();
				writer.write(conf.getTmpDir());
				writer.close();
				// write succeed, now move file to the right place, update
				// the versionset and commitlog

				File tmpFile = SSTableWriter.idxFile(conf.getTmpDir(), writer.getMeta());
				tmpFile.renameTo(SSTableWriter.idxFile(conf.getIndexDir(), writer.getMeta()));
				tmpFile = SSTableWriter.dirMetaFile(conf.getTmpDir(), writer.getMeta());
				tmpFile.renameTo(SSTableWriter.dirMetaFile(conf.getIndexDir(), writer.getMeta()));
				tmpFile = SSTableWriter.dataFile(conf.getTmpDir(), writer.getMeta());
				tmpFile.renameTo(SSTableWriter.dataFile(conf.getIndexDir(), writer.getMeta()));
				index.compactTables(new HashSet<SSTableMeta>(toCompact), writer.getMeta());
			} catch (IOException e) {
				logger.error(e.getStackTrace());
				throw new RuntimeException(e);
			}
		}
		snapshot = null;
		return ret;
	}

	private void markAsDel(List<SSTableMeta> toCompact) throws IOException {
		for (SSTableMeta meta : toCompact) {
			File file = SSTableWriter.dataFile(index.getConf().getIndexDir(), meta);
			FileUtil.markDel(file);
		}
	}
}
