package core.index.octree;

import java.util.concurrent.atomic.AtomicInteger;

import common.IntegerUtil;
import common.MidSegment;

import core.commom.Point;
import core.index.LogStructureOctree.OctreeMeta;

/**
 * the in-memory implementation of octree:
 * edges are power of 2
 * @author xiafan
 *
 */
public class MemoryOctree {
	private static int size_threshold = 10;
	OctreeMeta meta;
	OctreeNode root;

	public MemoryOctree(OctreeMeta meta) {
		this.meta = meta;
	}

	public OctreeMeta getMeta() {
		return meta;
	}

	public int size() {
		return meta.size;
	}

	public void insert(Point point, MidSegment seg) {
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
