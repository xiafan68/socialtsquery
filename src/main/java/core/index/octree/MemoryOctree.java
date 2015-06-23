package core.index.octree;

import java.util.concurrent.atomic.AtomicBoolean;

import common.IntegerUtil;
import common.MidSegment;

import core.commom.Point;

/**
 * the in-memory implementation of octree:
 * edges are power of 2
 * @author xiafan
 *
 */
public class MemoryOctree {
	private static int size_threshold = 10;
	OctreeMeta meta;
	OctreeNode root = null;
	// once true, no data can be inserted any further
	private AtomicBoolean immutable = new AtomicBoolean(false);

	public static class OctreeMeta {
		// octant meta
		public Point cornerPoint = new Point(0, 0, 0);
		public int edgeLen = 1;
		public int size = 0;
		public int minTime = 0;
		public int maxTime = 0;
	}

	public MemoryOctree(OctreeMeta meta) {
		this.meta = meta;
	}

	public void immutable() {
		immutable.set(true);
	}

	public OctreeMeta getMeta() {
		return meta;
	}

	public int size() {
		return meta.size;
	}

	/**
	 * 
	 * @param point
	 * @param seg
	 * @return true if insert success
	 * false if current tree is immutable
	 */
	public boolean insert(Point point, MidSegment seg) {
		if (immutable.get()) {
			return false;
		}
		meta.size++;
		if (root == null || !root.contains(point)) {
			OctreeNode preRoot = root;

			int power = Math.max(IntegerUtil.firstNoneZero(point.getX()),
					IntegerUtil.firstNoneZero(point.getY()));
			int len = 1 << (Math.max(power,
					IntegerUtil.firstNoneZero(point.getZ())) + 1);
			root = new OctreeNode(new Point(0, 0, 0), len);

			if (preRoot != null) {
				if (preRoot.size() < size_threshold) {
					root.addSegs(preRoot.getSegs());
				} else {
					root.split();
					root.setChild(0, preRoot);
				}
			}
		}
		OctreeNode leaf = root.search(point);
		leaf.insert(point, seg);
		if (leaf.size() > size_threshold) {
			leaf.split();
		}
		return true;
	}

	public void visit(OctreeVisitor visit) {
		root.visit(visit);
	}

	public void print() {
		System.out.println("MemoryOctree [meta=" + meta.toString());
		OctreePrinter printer = new OctreePrinter();
		if (root != null)
			root.visit(printer);
	}
}
