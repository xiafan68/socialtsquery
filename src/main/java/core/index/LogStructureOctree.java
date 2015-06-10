package core.index;

import java.io.DataInput;
import java.io.DataOutput;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import common.MidSegment;

import core.commom.Point;
import core.index.octree.MemoryOctree;

public class LogStructureOctree {
	// load from manifest file, but need to be verified against actual data
	public static class LSMOMeta {
		String word;
		int maxVersion = 0;
		int maxEdgeLen = 1;
		List<OctreeMeta> metas;

		public void serialize(DataOutput output) {

		}

		public void deserialize(DataInput input) {

		}
	}

	public static class OctreeMeta {
		// octant meta
		public int version = 0;
		public Point cornerPoint = new Point(0, 0, 0);
		public int edgeLen = 1;
		public int size = 0;// number of items
		// runtime state
		public AtomicInteger ref = new AtomicInteger(0);// reference for disk
														// file
		public AtomicBoolean del = new AtomicBoolean(false);// whether the disk
															// file has been
															// marked as
		// deleted
		// after compaction
	}

	LSMOMeta meta;
	MemoryOctree curTree;
	AtomicInteger ref = new AtomicInteger(0);
	// the set of
	public ConcurrentSkipListSet<OctreeMeta> diskTreeMetas = new ConcurrentSkipListSet<OctreeMeta>();
	public List<MemoryOctree> flushingTrees = new LinkedList<MemoryOctree>();
	OperationLog opLog;

	public void insert(MidSegment seg) {
		curTree.insert(seg.getPoint(), seg);
		if (curTree.size() > 100000) {
			flushMemOctree();
		}
	}

	/**
	 * 将当前的memory octree写入到磁盘
	 */
	public void flushMemOctree() {
		synchronized (flushingTrees) {
			if (curTree.getMeta().version == meta.maxVersion) {
				flushingTrees.add(curTree);
				OctreeMeta ometa = new OctreeMeta();
				ometa.version = meta.maxVersion++;
				curTree = new MemoryOctree(ometa);
			}
		}
	}

	public List<MemoryOctree> getFlushTrees() {
		return new LinkedList<MemoryOctree>();
	}

	public void flushedTrees(int num) {

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
}
