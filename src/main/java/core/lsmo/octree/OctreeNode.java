package core.lsmo.octree;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.TreeSet;

import common.MidSegment;
import core.commom.Encoding;
import core.commom.Point;
import io.ByteUtil;

/**
 * 对于这个cube，它不包含最右侧，最内侧和最上侧的面上的点
 * 
 * @author xiafan
 *
 */
public class OctreeNode {
	public static SerializeStrategy HANDLER;

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
		this.code = null;
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

	public boolean insert(Point point, MidSegment seg) {
		boolean ret = true;
		if (segs.contains(seg)) {
			// System.err.println(seg.toString());
			ret = false;
		}
		segs.add(seg);
		return ret;
	}

	/**
	 * 判断一个cube是否包含另一个point
	 * 
	 * @param point
	 * @return
	 */
	public boolean contains(Point point) {
		if (cornerPoint.getZ() <= point.getZ() && point.getZ() < cornerPoint.getZ() + edgeLen
				&& cornerPoint.getX() <= point.getX() && point.getX() < cornerPoint.getX() + edgeLen
				&& cornerPoint.getY() <= point.getY() && point.getY() < cornerPoint.getY() + edgeLen) {
			return true;
		}
		return false;
	}

	/**
	 * 判断一个node是否完全包含另外一个node，两个node也可能对应的方形相同
	 * 
	 * @param node
	 * @return
	 */
	public boolean contains(OctreeNode node) {
		Point point = node.getCornerPoint();
		int oLen = node.getEdgeLen();
		if (cornerPoint.getZ() <= point.getZ() && point.getZ() + oLen <= cornerPoint.getZ() + edgeLen
				&& cornerPoint.getX() <= point.getX() && point.getX() + oLen <= cornerPoint.getX() + edgeLen
				&& cornerPoint.getY() <= point.getY() && point.getY() + oLen <= cornerPoint.getY() + edgeLen) {
			return true;
		}
		return false;
	}

	/**
	 * 如果大部分点都在下半部分，那么有必要分裂
	 * 
	 * @return
	 */
	public int[] histogram() {
		int[] counters = new int[] { 0, 0 };
		int splitPanel = cornerPoint.getZ() + (edgeLen >> 1);
		for (MidSegment seg : segs) {
			if (seg.getPoint().getZ() >= splitPanel) {
				counters[1]++;
			} else {
				counters[0]++;
			}
		}
		return counters;
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

	public void write(DataOutput output) throws IOException {
		HANDLER.write(this, output);
	}

	public void read(DataInput input) throws IOException {
		HANDLER.read(this, input);
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

	private static interface SerializeStrategy {
		public void write(OctreeNode node, DataOutput output) throws IOException;

		public void read(OctreeNode node, DataInput input) throws IOException;
	}

	public static class PlainSerializer implements SerializeStrategy {

		@Override
		public void write(OctreeNode node, DataOutput output) throws IOException {
			output.writeInt(node.segs.size());
			for (MidSegment seg : node.segs) {
				seg.write(output);
			}
		}

		@Override
		public void read(OctreeNode node, DataInput input) throws IOException {
			int size = input.readInt();
			node.segs = new TreeSet<MidSegment>();
			for (int i = 0; i < size; i++) {
				MidSegment seg = new MidSegment();
				seg.readFields(input);
				node.segs.add(seg);
			}
		}

	}

	/**
	 * 采用压缩方法
	 * 
	 * @author xiafan
	 *
	 */
	public static enum CompressedSerializer implements SerializeStrategy {
		INSTANCE;
		@Override
		public void write(OctreeNode node, DataOutput output) {
			try {
				ByteUtil.writeVInt(output, node.segs.size());
				for (MidSegment seg : node.segs) {
					output.writeLong(seg.getMid());
					ByteUtil.writeVInt(output, seg.getStart());
					ByteUtil.writeVInt(output, seg.getEndTime() - seg.getStart());
					ByteUtil.writeVInt(output, seg.getStartCount());
					ByteUtil.writeVInt(output, seg.getEndCount());
				}
			} catch (IOException e) {
			}
		}

		@Override
		public void read(OctreeNode node, DataInput input) {
			try {
				int size = ByteUtil.readVInt(input);
				node.segs = new TreeSet<MidSegment>();
				for (int i = 0; i < size; i++) {
					MidSegment seg = new MidSegment();
					seg.mid = input.readLong();
					seg.setStart(ByteUtil.readVInt(input));
					seg.setEndTime(ByteUtil.readVInt(input) + seg.getStart());
					seg.setCount(ByteUtil.readVInt(input));
					seg.setEndCount(ByteUtil.readVInt(input));
					node.segs.add(seg);
				}
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}

	}
}
