package core.index;

import java.io.IOException;
import java.util.concurrent.ConcurrentSkipListSet;

import core.index.LogStructureOctree.OctreeMeta;
import core.index.octree.IOctreeIterator;
import core.index.octree.MemoryOctree;
import core.index.octree.OctreeIterator;
import core.index.octree.OctreeMerger;
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

	public void compactTrees() {
		// find out trees needing compaction
		try {
			OctreeMeta metal, metab;

			OctreeMeta newMeta = new OctreeMeta();
			IOctreeIterator treeIter = null;

			metal.markAsDel.set(true);
			metab.markAsDel.set(true);

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
