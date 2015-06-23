package core.index;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import common.MidSegment;

import core.commom.Point;
import core.index.octree.IOctreeIterator;
import core.index.octree.MemoryOctree;
import core.index.octree.OctreeIterator;
import core.index.octree.OctreeMerger;
import core.index.octree.OctreeZOrderBinaryWriter;

/**
 * the thread safety of this class is guaranteed by the client
 * @author xiafan
 *
 */
public class LogStructureOctree {
	// this field need to be loaded when coming into memory
	String keyword;
	VersionSet curVersion;
	MemoryOctree curTree;
	AtomicInteger ref = new AtomicInteger(0);
	CommitLog opLog;

	public LogStructureOctree() {

	}

	/**
	 * record the current memory tree, flushing memory tree and disk trees
	 * @author xiafan
	 *
	 */
	public static class VersionSet {
		public MemoryOctree curTree;
		public List<MemoryOctree> flushingTrees = new ArrayList<MemoryOctree>();

		public List<OctreeMeta> diskTreeMetas = new ArrayList<OctreeMeta>();

		// public HashMap<OctreeMeta>

		/**
		 * client grantuee thread safe, used when flushing current octree
		 * @param newTree
		 */
		public void newMemTree(MemoryOctree newTree) {
			curTree.immutable();
			flushingTrees.add(curTree);
			curTree = newTree;
		}

		/**
		 * used when a set of memory octree are flushed to disk as one single unit
		 * @param versions
		 * @param newMeta
		 */
		public void flush(List<Integer> versions, OctreeMeta newMeta) {

		}

		/**
		 * used when a set of version a compacted
		 * @param versions
		 * @param newMeta
		 */
		public void compact(List<Integer> versions, OctreeMeta newMeta) {

		}
	}

	public void insert(MidSegment seg) {
		opLog.write(keyword, seg);
		curTree.insert(seg.getPoint(), seg);
	}

	/**
	 * 判断version是否已经写出到磁盘上
	 * @param version
	 * @return
	 */
	public boolean isVersionFlushed(int version) {
		// TODO
		return true;
	}

	public boolean increRef() {
		int ret = ref.incrementAndGet();
		if (ret < 0)
			return true;
		return false;
	}

	public void decreRef() {
		ref.decrementAndGet();
	}

	/**
	 * release the logstructureoctree succesed, it can be removed from memory now
	 * @return
	 */
	public boolean release() {
		return ref.compareAndSet(0, Integer.MIN_VALUE);
	}

	public File getDataDir() {
		// TODO Auto-generated method stub
		return null;
	}

	/**
	 * 将当前的memory octree写入到磁盘
	 */
	public void flushMemOctree(int newVersion) {
		OctreeMeta ometa = new OctreeMeta();
		ometa.version = newVersion;
		curTree = new MemoryOctree(ometa);
		curVersion.newMemTree(curTree);
	}

	public void flushedTrees() {
		try {
			List<MemoryOctree> trees = new LinkedList<MemoryOctree>(
					curVersion.flushingTrees);
			IOctreeIterator treeIter = null;

			int version = 0;
			for (MemoryOctree curTree : trees) {
				version = Math.max(curTree.getMeta().version, version);
			}

			if (trees.size() == 1) {
				treeIter = new OctreeIterator(trees.get(0));
			} else if (trees.size() >= 2) {
				treeIter = new OctreeIterator(trees.get(0));
				for (MemoryOctree curTree : trees.subList(1, trees.size())) {
					treeIter = new OctreeMerger(treeIter, new OctreeIterator(
							curTree));
				}
			}
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
