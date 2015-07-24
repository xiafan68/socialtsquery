package core.index;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;

import Util.Pair;
import core.commom.Encoding;
import core.commom.Point;
import core.index.SSTableWriter.DirEntry;
import core.index.octree.IOctreeIterator;
import core.index.octree.MemoryOctree.OctreeMeta;
import core.index.octree.OctreeNode;
import core.io.Bucket;
import core.io.Bucket.BucketID;
import fanxia.file.ByteUtil;

public class OctreePostingListIter implements IOctreeIterator {
	private DirEntry entry;
	private SSTableReader reader;
	private int ts;
	private int te;

	private Encoding curCode = null;
	private BucketID curID = null;
	private Bucket curBuck = new Bucket(0);
	private OctreeNode curNode = new OctreeNode(null, 0);
	private int nextID = 0;

	private enum SkipType {
		floor, cell
	};

	private SkipType skipType;

	/**
	 * 
	 * @param reader
	 * @param ts
	 * @param te
	 */
	public OctreePostingListIter(DirEntry entry, SSTableReader reader, int ts,
			int te) {
		this.entry = entry;
		this.reader = reader;
		this.ts = ts;
		this.te = te;
		curCode = new Encoding(new Point(ts, ts, Integer.MAX_VALUE), 0);
	}

	@Override
	public OctreeMeta getMeta() {
		return entry;
	}

	@Override
	public void addNode(OctreeNode node) {
		throw new RuntimeException("not supported");
	}

	@Override
	public void open() throws IOException {

	}

	@Override
	public boolean hasNext() throws IOException {
		// TODO Auto-generated method stub
		return false;
	}

	private void gotoNewLayer(int layer) {
		curCode.setX(ts);
		curCode.setY(te);
		curCode.setZ(layer);
		curBuck = null;
	}

	private boolean isCorrectCode(Encoding newCode) {
		// go to the next layer
		if (newCode.getZ() != curCode.getZ()) {
			gotoNewLayer(newCode.getZ());
		} else if (newCode.getX() > te) {
			gotoNewLayer(newCode.getZ() - 1);
		} else if (newCode.getY() < ts) {
			int commBit = ByteUtil.commonBits(newCode.getY(), ts);
			int newY = ByteUtil.commonBits(newCode.getY(), commBit);
			int mask = 1 << (31 - commBit);
			newY |= mask;
			int newZ = ByteUtil.commonBits(newCode.getX(), commBit);
			curCode.setY(newY);
			curCode.setZ(newZ);
		} else {
			return true;
		}
		return false;
	}

	/**
	 * 
	 * @throws IOException
	 */
	private void skipTo() throws IOException {
		Pair<Encoding, BucketID> pair = null;
		
		while(){
		pair = reader.cellOffset(entry.curKey, curCode);
		if (pair == null)
			pair = reader.floorOffset(entry.curKey, curCode);
		if (pair == null) {
			// advance to next bucket
			nextID = reader.getBucket(curID, curBuck);
			curID.blockID = nextID;
			curID.offset = 0;
			nextID = -1;
		} else {
			if (isCorrectCode(pair.getKey())) {
				curID = pair.getValue();
			}
		}
		}
	}

	private void nextOctant() throws IOException {
		while (curNode != null) {
			if (curBuck != null) {
				byte[] data = curBuck.getOctree(curID.offset);
				DataInputStream input = new DataInputStream(
						new ByteArrayInputStream(data));
				curCode.readFields(input);
				if (isCorrectCode(curCode)) {
					curNode.setPoint(curCode);
					curNode.read(input);
					break;
				}
			} else {
				Pair<Encoding, BucketID> pair = null;
				if (skipType == SkipType.cell) {
					pair = reader.cellOffset(entry.curKey, curCode);
				} else {
					pair = reader.floorOffset(entry.curKey, curCode);
				}

				if (pair == null) {
					// advance to next bucket
					nextID = reader.getBucket(curID, curBuck);
					curID.blockID = nextID;
					curID.offset = 0;
					nextID = -1;
				} else {
					if (isCorrectCode(pair.getKey())) {
						curID = pair.getValue();
					}
				}
			}
		}
	}

	@Override
	public OctreeNode next() throws IOException {

		return null;
	}

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub

	}

}
