package core.lsmo.persistence;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import core.commom.Writable;
import core.commom.WritableComparable;
import core.commom.WritableComparable.WritableComparableFactory;
import core.io.Block;
import core.io.Block.BLOCKTYPE;
import core.io.Bucket.BucketID;
import io.ByteUtil;
import util.Pair;

/**
 * metablock的格式为：
 * ______________________________________________________________________
 * nextMetaBlockIdx
 * ______________________________________________________________________ number
 * of indices
 * ______________________________________________________________________
 * key+bucketid
 * ______________________________________________________________________
 * key+bucketid
 * 
 * @author xiafan
 *
 */
public class SkipCell implements Writable {
	public int blockIdx;
	public int nextMetaBlockIdx = 0;// 下一个meta block的地址
	WritableComparableFactory factory;

	// 以下字段用于写索引
	ByteArrayOutputStream bout;
	DataOutputStream metaDos = null;
	int size = 0;

	ByteArrayOutputStream lastIndexBos;
	DataOutputStream lastIndexDos = null;

	// 当从文件中都出时反序列化出以下字段
	List<Pair<WritableComparable, BucketID>> skipList = new ArrayList<Pair<WritableComparable, BucketID>>();

	public SkipCell(int blockIdx, WritableComparableFactory keyFactory) {
		this.blockIdx = blockIdx;
		this.factory = keyFactory;
		bout = new ByteArrayOutputStream(Block.availableSpace());
		metaDos = new DataOutputStream(bout);
		lastIndexBos = new ByteArrayOutputStream();
		lastIndexDos = new DataOutputStream(lastIndexBos);
		reset();
	}

	public int getBlockIdx() {
		return blockIdx;
	}

	public long toFileOffset() {
		return (((long) getBlockIdx()) << 32) | (size() - 1);
	}

	public void setBlockIdx(int blockIdx) {
		this.blockIdx = blockIdx;
	}

	public Pair<WritableComparable, BucketID> getIndexEntry(int idx) {
		return skipList.get(idx);
	}

	public int size() {
		return skipList.size() > 0 ? skipList.size() : size;
	}

	public int cellOffset(WritableComparable key, int start, int end) {
		Pair<WritableComparable, BucketID> pair = new Pair<WritableComparable, BucketID>(key, null);
		int idx = Collections.binarySearch(skipList.subList(start, end), pair,
				new Comparator<Pair<WritableComparable, BucketID>>() {
					@Override
					public int compare(Pair<WritableComparable, BucketID> o1, Pair<WritableComparable, BucketID> o2) {
						return o1.getKey().compareTo(o2.getKey());
					}
				});
		if (idx < 0) {
			idx = Math.abs(1 + idx) - 1;// 找到第一个小于key的索引项
			idx = idx < 0 ? 0 : idx;
		}
		idx += start;
		return idx;
	}

	public int floorOffset(WritableComparable curKey, int start, int end) {
		Pair<WritableComparable, BucketID> pair = new Pair<WritableComparable, BucketID>(curKey, null);
		int idx = Collections.binarySearch(skipList.subList(start, end), pair,
				new Comparator<Pair<WritableComparable, BucketID>>() {
					@Override
					public int compare(Pair<WritableComparable, BucketID> o1, Pair<WritableComparable, BucketID> o2) {
						return o1.getKey().compareTo(o2.getKey());
					}
				});
		if (idx < 0) {
			idx = Math.abs(1 + idx);// 找到第一个小于key的索引项
		}
		idx += start;
		if (idx == end)
			idx = -1;
		return idx;
	}

	/**
	 * 重置当前cell，用于开始新的写入
	 */
	public void reset() {
		try {
			bout.reset();
			metaDos = new DataOutputStream(bout);
			metaDos.writeInt(nextMetaBlockIdx);
			metaDos.writeInt(0);
			size = 0;
			skipList.clear();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * 将新的code, offfset加入到metablock中，如果失败的话返回false;
	 * 
	 * @param code
	 * @param bOffset
	 * @param octOffset
	 * @return
	 * @throws IOException
	 */
	private boolean addIndex(WritableComparable code, int bOffset, short octOffset) throws IOException {
		if (metaDos == null) {
			reset();
		}
		// if the dos is overflow, this item should be removed from the stream
		code.write(lastIndexDos);
		ByteUtil.writeVInt(lastIndexDos, bOffset);
		ByteUtil.writeVInt(lastIndexDos, octOffset);

		boolean ret = lastIndexDos.size() + metaDos.size() <= Block.availableSpace();
		if (ret) {
			size++;
			lastIndexDos.flush();
			metaDos.write(lastIndexBos.toByteArray());
		}
		lastIndexBos.reset();
		lastIndexDos = new DataOutputStream(lastIndexBos);
		return ret;
	}

	public boolean addIndex(WritableComparable code, BucketID indexBlockIdx) throws IOException {
		int bOffset = indexBlockIdx.blockID - blockIdx - 1;
		return addIndex(code, bOffset, indexBlockIdx.offset);
	}

	public void read(Block metaBlock) throws IOException {
		ByteArrayInputStream binput = new ByteArrayInputStream(metaBlock.getData());
		DataInputStream input = new DataInputStream(binput);
		nextMetaBlockIdx = input.readInt();
		size = input.readInt();
		for (int i = 0; i < size; i++) {
			WritableComparable key = factory.create();
			key.read(input);
			BucketID bucket = new BucketID();
			bucket.read(input);
			skipList.add(new Pair<WritableComparable, BucketID>(key, bucket));
		}
	}

	@Override
	public void read(DataInput input) throws IOException {
		Block metaBlock = new Block(BLOCKTYPE.META_BLOCK, -1);
		metaBlock.read(input);
		read(metaBlock);
	}

	@Override
	public void write(DataOutput output) throws IOException {
		write(-1).write(output);
	}

	public Block write(int nextMetaBlockIdx) throws IOException {
		this.nextMetaBlockIdx = nextMetaBlockIdx;
		byte[] metaArray = bout.toByteArray();
		ByteUtil.writeInt(nextMetaBlockIdx, metaArray, 0);
		ByteUtil.writeInt(size, metaArray, 4);
		Block metaBlock = new Block(BLOCKTYPE.META_BLOCK, blockIdx);
		metaBlock.setData(metaArray);
		return metaBlock;
	}

	private void deserialize() throws IOException {
		if (skipList.size() == 0 && size > 0) {
			DataInputStream input = new DataInputStream(new ByteArrayInputStream(bout.toByteArray()));
			input.readInt();
			input.readInt();
			for (int i = 0; i < size; i++) {
				WritableComparable key = factory.create();
				key.read(input);
				BucketID bucket = new BucketID();
				bucket.read(input);
				skipList.add(new Pair<WritableComparable, BucketID>(key, bucket));
			}
		}
	}

	@Override
	public boolean equals(Object other) {
		SkipCell oCell = (SkipCell) other;
		try {
			this.deserialize();
			oCell.deserialize();
			return nextMetaBlockIdx == oCell.nextMetaBlockIdx && this.skipList.equals(oCell.skipList);
		} catch (IOException e) {
		}
		return false;
	}
}