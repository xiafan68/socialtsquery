package core.index;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentSkipListSet;

import core.index.LSMOInvertedIndex.VersionSet;
import core.index.MemTable.SSTableMeta;
import core.index.octree.OctreeZOrderBinaryWriter;

/**
 * to avoid scan disk to find those posting lists needing compaction, a lazy
 * policy is adopted. Only posting list that has come into memory will be
 * checked for compaction
 * 
 * @author xiafan
 * 
 */
public class CompactService {
	LSMOInvertedIndex index;
	ConcurrentSkipListSet<String> compactingWords = new ConcurrentSkipListSet<String>();

	public CompactService(LSMOInvertedIndex index) {
		this.index = index;
	}

	public void start() {

	}

	public void stop() {

	}

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

	public void compactTrees() {
		// find out trees needing compaction
		//TODO 1. 确定需要压缩的文件。2. 获取相关的SSTableReader， 3. 使用SSTableWriter写出数据 4. 使用Index更新版本集合。
		try {
			List<SSTableMeta> toCompact = fileToCompact();
			if (!toCompact.isEmpty()){
				List<SSTableReader> readers = new ArrayList<SSTableReader>();
				for(SSTableMeta meta : toCompact){
				index.getSSTableReader()
				}
			SSTableWriter writer = new SSTableWriter();
			OctreeZOrderBinaryWriter writer = new OctreeZOrderBinaryWriter(
					getDataDir(), version, treeIter);
			writer.open();
			try {
				writer.write();
				// TODO synchronization here
				curVersion.flushingTrees.subList(0, trees.size()).clear();
				opLog.markFlushed(version);
			} finally {
				writer.close();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		}
	}
}
