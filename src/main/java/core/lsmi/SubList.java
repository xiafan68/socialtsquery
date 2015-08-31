package core.lsmi;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.output.ByteArrayOutputStream;

import segmentation.Segment;

import common.MidSegment;

import fanxia.file.ByteUtil;

/**
 * 
 * @author xiafan
 *
 */
public class SubList {
	final int sizeLimit;
	List<MidSegment> segs = new ArrayList<MidSegment>();

	public SubList(int sizeLimit) {
		this.sizeLimit = sizeLimit;
	}

	public void init() {
		segs.clear();
	}

	public void addSegment(MidSegment seg) {
		segs.add(seg);
	}

	public boolean isFull() {
		return segs.size() >= sizeLimit;
	}

	public byte[] toByteArray() throws IOException {
		ByteArrayOutputStream output = new ByteArrayOutputStream();
		DataOutputStream dos = new DataOutputStream(output);
		ByteUtil.writeVInt(dos, segs.size());
		for (MidSegment seg : segs) {
			seg.write(dos);
		}
		return output.toByteArray();
	}

	public void read(DataInput input) throws IOException {
		int size = ByteUtil.readVInt(input);
		segs.clear();
		for (int i = 0; i < size; i++) {
			MidSegment seg = new MidSegment();
			seg.readFields(input);
			segs.add(seg);
		}
	}

	public MidSegment get(int i) {
		return segs.get(i);
	}

	public boolean isEmpty() {
		return segs.isEmpty();
	}

	public int size() {
		return segs.size();
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "SubList [sizeLimit=" + sizeLimit + ", segs=" + segs + "]";
	}

	public static void main(String[] args) throws IOException {
		SubList list = new SubList(10);
		for (int i = 0; i < 10; i++) {
			MidSegment seg = new MidSegment(i + 10, new Segment(i + 10, i,
					i + 10, i + 1));
			list.addSegment(seg);
		}
		SubList bList = new SubList(10);
		bList.read(new DataInputStream(new ByteArrayInputStream(list
				.toByteArray())));

		System.out.println(list);
		System.out.println(bList);
	}
}
