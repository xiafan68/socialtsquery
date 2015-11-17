package core.lsmo.octree;

import java.util.concurrent.atomic.AtomicBoolean;

import common.MidSegment;
import core.commom.Point;
import core.lsmt.PostingListMeta;

/**
 * the in-memory implementation of octree: edges are power of 2
 * 需要修改实现，将MemoryOctree的root的大小设置为最小的valid bounding octant
 * 
 * @author xiafan
 *
 */
public class MemoryOctree {
	public final int size_threshold; // MidSegment的大小为24byte，如果压缩的话更小，因此这个100略小
	PostingListMeta meta;

	Point lower = new Point(Integer.MAX_VALUE, Integer.MAX_VALUE, Integer.MAX_VALUE);
	Point upper = new Point(0, 0, 0);

	OctreeNode root = null;
	// once true, no data can be inserted any further
	private AtomicBoolean immutable = new AtomicBoolean(false);

	public MemoryOctree(PostingListMeta meta) {
		this.meta = meta;
		size_threshold = 350;
	}

	public MemoryOctree(PostingListMeta postingListMeta, int octantSizeLimit) {
		this.meta = postingListMeta;
		size_threshold = octantSizeLimit;
	}

	public void immutable() {
		immutable.set(true);
	}

	public PostingListMeta getMeta() {
		return meta;
	}

	public int size() {
		return meta.size;
	}

	private void genNewRoot(Point point) {
		lower.setX(Math.min(lower.getX(), point.getX()));
		lower.setY(Math.min(lower.getY(), point.getY()));
		lower.setZ(Math.min(lower.getZ(), point.getZ()));
		upper.setX(Math.max(upper.getX(), point.getX()));
		upper.setY(Math.max(upper.getY(), point.getY()));
		upper.setZ(Math.max(upper.getZ(), point.getZ()));

		int paddingBit = 0;
		int mask = 0xffffffff;
		int len = (1 << paddingBit) - 1;
		int newX = lower.getX();
		int newY = lower.getY();
		int newZ = lower.getZ();
		do {
			newX &= mask;
			newY &= mask;
			newZ &= mask;
			if (newX + len >= upper.getX() && newY + len >= upper.getY() && newZ + len >= upper.getZ()) {
				break;
			}
			mask <<= 1;
			paddingBit++;
			len = (1 << paddingBit) - 1;
		} while (true);
		root = new OctreeNode(new Point(newX, newY, newZ), len + 1);
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

			// int power = Math.max(IntegerUtil.firstNoneZero(point.getX()),
			// IntegerUtil.firstNoneZero(point.getY()));
			// int len = 1 << (Math.max(power,
			// IntegerUtil.firstNoneZero(point.getZ())) + 1);
			// root = new OctreeNode(new Point(0, 0, 0), len);
			genNewRoot(point);

			if (preRoot != null) {
				if (preRoot.isLeaf()) {
					root.addSegs(preRoot.getSegs());
				} else {
					OctreeNode cur = root;
					boolean preRootInserted = false;
					do {
						cur.split();
						for (int i = 0; i < 8; i++) {
							if (cur.getChild(i).getEncoding().compareTo(preRoot.getEncoding()) == 0) {
								cur.setChild(i, preRoot);
								preRootInserted = true;
								break;
							} else if (cur.getChild(i).contains(preRoot)) {
								cur = cur.getChild(i);
								break;
							}
						}
						/*
						 * if (cur.getChild(0).getEncoding().compareTo(preRoot.
						 * getEncoding()) == 0) { cur.setChild(0, preRoot);
						 * break; } else { cur = cur.getChild(0); }
						 */
					} while (!preRootInserted);
				}
			}
		}
		OctreeNode leaf = root.search(point);
		leaf.insert(point, seg);
		splitNode(leaf);
		return true;
	}

	private void splitNode(OctreeNode node) {
		if (node.getEdgeLen() > 1 && node.size() > size_threshold) {
			node.split();
			for (int i = 0; i < 8; i++) {
				splitNode(node.getChild(i));
			}
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
