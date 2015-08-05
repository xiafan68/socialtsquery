package core.index.octree;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.TreeSet;

import common.MidSegment;

import core.commom.Encoding;
import core.commom.Point;
import fanxia.file.ByteUtil;

public class OctreeNode {
	int edgeLen;
	Point cornerPoint;
	OctreeNode[] children = null;
	TreeSet<MidSegment> segs;
	Encoding code = null;

	public OctreeNode(Point cornerPoint, int edgeLen) {
		this.cornerPoint = cornerPoint;
		this.edgeLen = edgeLen;
		segs = new TreeSet<MidSegment>();
	}

	public void setPoint(Point cornerPoint) {
		this.cornerPoint = cornerPoint;
	}

	public OctreeNode search(Point point) {
		if (!isLeaf()) {
			int idx = 0;
			if (point.getZ() >= cornerPoint.getZ() + edgeLen / 2)
				idx |= 0x4;
			if (point.getX() >= cornerPoint.getX() + edgeLen / 2)
				idx |= 0x1;
			if (point.getY() >= cornerPoint.getY() + edgeLen / 2)
				idx |= 0x2;
			return children[idx].search(point);
		} else {
			return this;
		}
	}

	public void insert(Point point, MidSegment seg) {
		segs.add(seg);
	}

	public boolean contains(Point point) {
		if (cornerPoint.getZ() <= point.getZ() && point.getZ() < cornerPoint.getZ() + edgeLen
				&& cornerPoint.getX() <= point.getX() && point.getX() < cornerPoint.getX() + edgeLen
				&& cornerPoint.getY() <= point.getY() && point.getY() < cornerPoint.getY() + edgeLen) {
			return true;
		}
		return false;
	}

	public boolean contains(OctreeNode node) {
		Point point = node.getCornerPoint();
		int oLen = node.getEdgeLen();
		if (cornerPoint.getZ() <= point.getZ() && point.getZ() + oLen < cornerPoint.getZ() + edgeLen
				&& cornerPoint.getX() <= point.getX() && point.getX() + oLen < cornerPoint.getX() + edgeLen
				&& cornerPoint.getY() <= point.getY() && point.getY() + oLen < cornerPoint.getY() + edgeLen) {
			return true;
		}
		return false;
	}

	public void split() {
		children = new OctreeNode[8];
		for (int i = 0; i < children.length; i++) {
			int x = ((i & 0x1) > 0) ? edgeLen / 2 : 0;
			int y = ((i & 0x2) > 0) ? edgeLen / 2 : 0;
			int z = ((i & 0x4) > 0) ? edgeLen / 2 : 0;
			Point childCorner = new Point(cornerPoint.getX() + x, cornerPoint.getY() + y, cornerPoint.getZ() + z);
			children[i] = new OctreeNode(childCorner, edgeLen / 2);
		}

		for (MidSegment seg : segs) {
			Point p = seg.getPoint();
			search(p).insert(p, seg);
		}

		segs.clear();
		segs = null;
	}

	public void setChild(int idx, OctreeNode child) {
		children[idx] = child;
	}

	public OctreeNode getChild(int idx) {
		return children[idx];
	}

	/**
	 * how to map multiple nodes to a bucket? a bucket may consists of multiple
	 * blocks
	 * 
	 * @param writer
	 */
	public void visit(OctreeVisitor writer) {
		if (isLeaf())
			writer.visitLeaf(this);
		else
			writer.visitIntern(this);
	}

	public boolean isLeaf() {
		return children == null;
	}

	/**
	 * for the z dimension, we use the upper face in the encoding
	 * 
	 * @return
	 */
	public Encoding getEncoding() {
		if (code == null) {
			code = new Encoding(new Point(cornerPoint.getX(), cornerPoint.getY(), cornerPoint.getZ()),
					ByteUtil.lastNonZeroBitFromTail(edgeLen) + 1);
		}
		return code;
	}

	public void write(DataOutput output) {
		try {
			output.writeInt(segs.size());
			for (MidSegment seg : segs) {
				seg.write(output);
			}
		} catch (IOException e) {
		}
	}

	public void read(DataInput input) {
		int size;
		try {
			size = input.readInt();
			segs = new TreeSet<MidSegment>();
			for (int i = 0; i < size; i++) {
				MidSegment seg = new MidSegment();
				seg.readFields(input);
				segs.add(seg);
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * @return the edgeLen
	 */
	public int getEdgeLen() {
		return edgeLen;
	}

	/**
	 * @return the cornerPoint
	 */
	public Point getCornerPoint() {
		return cornerPoint;
	}

	/**
	 * @return the segs
	 */
	public TreeSet<MidSegment> getSegs() {
		return segs;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "OctreeNode [children=" + Arrays.toString(children) + ", segs=" + segs + " code:" + getEncoding() + "]";
	}

	public static void main(String[] args) {
		OctreeNode cur = new OctreeNode(new Point(0, 0, 0), 1 << 10);
		System.out.println(cur);
		cur.split();
		System.out.println(cur);
	}

	public int size() {
		if (segs != null)
			return segs.size();
		return 0;
	}

	public void addSegs(TreeSet<MidSegment> items) {
		children = null;
		segs.addAll(items);
	}
}
