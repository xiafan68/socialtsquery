package core.index;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentSkipListSet;

import core.index.octree.IOctreeIterator;
import core.index.octree.MemoryOctree;
import core.index.octree.OctreeIterator;
import core.index.octree.OctreeMerger;
import core.index.octree.OctreeZOrderBinaryWriter;

/**
 * responsible for write data to disk
 * @author xiafan
 *
 */
public class FlushService {
	LSMOInvertedIndex index;
	ConcurrentSkipListSet<String> pendingWords = new ConcurrentSkipListSet<String>();
	ConcurrentSkipListSet<String> flushingWords = new ConcurrentSkipListSet<String>();

	private class FlushWorker extends Thread {
		public FlushWorker() {
			super("flush worker");
		}

		@Override
		public void run() {
			while (index.running.get()) {
				Iterator<String> iter = pendingWords.iterator();
				while (iter.hasNext()) {
					String word = iter.next();
					iter.remove();
					if (flushingWords.add(word)) {
						LogStructureOctree tree = index.getPostingList(word);
						if (tree != null) {
							try {
								List<MemoryOctree> trees = tree.getFlushTrees();
								IOctreeIterator treeIter = null;

								int version = 0;
								for (MemoryOctree curTree : trees) {
									version = Math.max(
											curTree.getMeta().version, version);
								}

								if (trees.size() == 1) {
									treeIter = new OctreeIterator(trees.get(0));
								} else if (trees.size() >= 2) {
									treeIter = new OctreeIterator(
											trees.remove(0));
									for (MemoryOctree curTree : trees) {
										treeIter = new OctreeMerger(treeIter,
												new OctreeIterator(curTree));
									}
								}
								OctreeZOrderBinaryWriter writer = new OctreeZOrderBinaryWriter(
										null, version, treeIter);
								writer.open();
								try {
									writer.write();
								} finally {
									writer.close();
								}
							} catch (IOException e) {
								e.printStackTrace();
							} finally {
								tree.decreRef();
							}
						}
						flushingWords.remove(word);
					}
				}
			}
		}
	}

	public FlushService(LSMOInvertedIndex index) {
		this.index = index;
	}

	/**
	 * an indication that posting list of word need to be flushed
	 * @param word
	 */
	public void register(String word) {
		pendingWords.add(word);
	}

}
