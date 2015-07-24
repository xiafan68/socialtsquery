package core.index;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentSkipListSet;

import Util.Configuration;
import core.index.LSMOInvertedIndex.VersionSet;
import core.index.MemTable.SSTableMeta;

/**
 * to avoid scan disk to find those posting lists needing compaction, a lazy
 * policy is adopted. Only posting list that has come into memory will be
 * checked for compaction
 * 
 * @author xiafan
 * 
 */
public class CompactService extends Thread {
	LSMOInvertedIndex index;
	ConcurrentSkipListSet<String> compactingWords = new ConcurrentSkipListSet<String>();

	public CompactService(LSMOInvertedIndex index) {
		super("compaction thread");
		this.index = index;
	}

	/**
	 * TODO
	 * @return
	 */
	private List<SSTableMeta> fileToCompact() {
		List<SSTableMeta> ret = new ArrayList<SSTableMeta>();
		VersionSet set = index.getVersion();
		TreeMap<Integer, Integer> levelNums = new TreeMap<Integer, Integer>();
		for (SSTableMeta meta : set.diskTreeMetas) {
			if (levelNums.containsKey(meta.level)) {
				levelNums.put(meta.level, levelNums.get(meta.level) + 1);
			} else {
				levelNums.put(meta.level, 1);
			}
		}
		return ret;
	}

	@Override
	public void run() {
		while (index.running.get()) {
			if (!compactTrees()) {
				try {
					Thread.sleep(100);
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
		if (!toCompact.isEmpty()) {
			ret = true;
			List<SSTableReader> readers = new ArrayList<SSTableReader>();
			for (SSTableMeta meta : toCompact) {
				readers.add(index.getSSTableReader(meta));
			}
			SSTableMeta newMeta = new SSTableMeta(toCompact.get(0).version,
					toCompact.get(0).level + 1);
			SSTableWriter writer = new SSTableWriter(newMeta, readers,
					index.getStep());

			try {
				Configuration conf = index.getConf();
				writer.write(conf.getTmpDir());
				writer.close();
				// write succeed, now move file to the right place, update the
				// versionset and commitlog

				File tmpFile = SSTableWriter.idxFile(conf.getTmpDir(),
						writer.getMeta());
				tmpFile.renameTo(SSTableWriter.idxFile(conf.getIndexDir(),
						writer.getMeta()));
				tmpFile = SSTableWriter.dirMetaFile(conf.getTmpDir(),
						writer.getMeta());
				tmpFile.renameTo(SSTableWriter.dirMetaFile(conf.getIndexDir(),
						writer.getMeta()));
				tmpFile = SSTableWriter.dataFile(conf.getTmpDir(),
						writer.getMeta());
				tmpFile.renameTo(SSTableWriter.dataFile(conf.getIndexDir(),
						writer.getMeta()));
				Set<Integer> versions = new HashSet<Integer>();
				index.compactTables(versions, writer.getMeta());
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}
		return ret;
	}
}
