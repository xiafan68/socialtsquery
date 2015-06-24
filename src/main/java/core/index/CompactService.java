package core.index;

import java.io.IOException;
import java.util.List;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentSkipListSet;

import core.index.LSMOInvertedIndex.VersionSet;
import core.index.MemTable.SSTableMeta;
import core.index.octree.IOctreeIterator;
import core.index.octree.MemoryOctree.OctreeMeta;
import core.index.octree.OctreeZOrderBinaryWriter;

/**
 * to avoid scan disk to find those posting lists needing compaction, a lazy policy is adopted. 
 * Only posting list that has come into memory will be checked for compaction
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
		VersionSet set = index.getVersion();
		TreeMap<Integer, Integer> levelNums = new TreeMap<Integer, Integer>();
		for (SSTableMeta meta : set.diskTreeMetas) {
			if (levelNums.containsKey(meta.level)) {
				levelNums.put(meta.level, levelNums.get(meta.level) + 1);
			} else {
				levelNums.put(meta.level, 1);
			}
		}
		
	}

	public void compactTrees() {
		// find out trees needing compaction
		try {
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
