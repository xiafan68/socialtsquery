package core.lsmo.octree;

import java.util.concurrent.atomic.AtomicBoolean;

import common.IntegerUtil;
import common.MidSegment;
import core.commom.Point;

/**
 * the in-memory implementation of octree: edges are power of 2
 * 
 * @author xiafan
 *
 */
public class MemoryOctree {
	public static int size_threshold = 100;
	OctreeMeta meta;
	OctreeNode root = null;
	// once true, no data can be inserted any further
	private AtomicBoolean immutable = new AtomicBoolean(false);

	public static class OctreeMeta {
		// octant meta
		// public Point cornerPoint = new Point(0, 0, 0);
		public int size = 0;
		public int maxTime = Integer.MIN_VALUE;
		public int minTime = Integer.MAX_VALUE;

		public OctreeMeta() {

		}

		public OctreeMeta(OctreeMeta a, OctreeMeta b) {
			size = a.size + b.size;
			minTime = Math.min(a.minTime, b.minTime);
			maxTime = Math.max(a.maxTime, b.maxTime);
		}
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
	 * @return true if insert success false if current tree is immutable
	 */
	public boolean insert(Point point, MidSegment seg) {
		if (immutable.get()) {
			return false;
		}
		meta.size++;
		meta.minTime = Math.min(meta.minTime, point.getX());
		meta.maxTime = Math.max(meta.maxTime, point.getY());
		if (root == null || !root.contains(point)) {
			OctreeNode preRoot = root;

			int power = Math.max(IntegerUtil.firstNoneZero(point.getX()),
					IntegerUtil.firstNoneZero(point.getY()));
			int len = 1 << (Math.max(power,
					IntegerUtil.firstNoneZero(point.getZ())) + 1);
			root = new OctreeNode(new Point(0, 0, 0), len);

			if (preRoot != null) {
				if (preRoot.isLeaf()) {
					root.addSegs(preRoot.getSegs());
				} else {
					OctreeNode cur = root;
					do {
						cur.split();
						if (cur.getChild(0).getEncoding()
								.compareTo(preRoot.getEncoding()) == 0) {
							cur.setChild(0, preRoot);
							break;
						} else {
							cur = cur.getChild(0);
						}
					} while (true);
					// root.split();
					// root.setChild(0, preRoot);
				}
			}
		}
		OctreeNode leaf = root.search(point);
		leaf.insert(point, seg);
		if (leaf.getEdgeLen() > 1 && leaf.size() > size_threshold) {
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
