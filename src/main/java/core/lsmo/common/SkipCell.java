package core.lsmo.common;

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

import core.io.Block;
import core.io.Block.BLOCKTYPE;
import core.io.Bucket.BucketID;
import core.lsmt.Writable;
import core.lsmt.WritableComparable;
import core.lsmt.WritableComparable.WritableComparableKeyFactory;
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
	Block metaBlock;
	int blockIdx;
	int nextMetaBlockIdx = 0;// 下一个meta block的地址
	WritableComparableKeyFactory factory;

	// 以下字段用于写索引
	ByteArrayOutputStream bout;
	DataOutputStream metaDos = null;
	// 用于存留最后一个bucket的数据
	ByteArrayOutputStream lastBuckBout;
	DataOutputStream lastBuckMetaDos;

	int size = 0;

	// 当从文件中都出时反序列化出以下字段
	List<Pair<WritableComparable, BucketID>> skipList = new ArrayList<Pair<WritableComparable, BucketID>>();

	public SkipCell(int blockIdx, WritableComparableKeyFactory factory) {
		this.blockIdx = blockIdx;
		this.factory = factory;
	}

	public int getBlockIdx() {
		return blockIdx;
	}

	public long toFileOffset() {
		return (((long) getBlockIdx()) << 32) | (size() - 1);
	}

	private void setupWriteStream() {
		bout = new ByteArrayOutputStream(Block.availableSpace());
		metaDos = new DataOutputStream(bout);
		lastBuckBout = new ByteArrayOutputStream();
		lastBuckMetaDos = new DataOutputStream(lastBuckBout);
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

	/**
	 * 如果最后一个bucket写出到缓冲区了，那么它的blockidx就确定了，这时当前skipcell中对应的索引项就可以写出去
	 * 
	 * @throws IOException
	 */
	public void newBucket() throws IOException {
		metaDos.write(lastBuckBout.toByteArray());
		lastBuckBout.reset();
		lastBuckMetaDos = new DataOutputStream(lastBuckBout);
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

	public void read(Block metaBlock) throws IOException {
		ByteArrayInputStream binput = new ByteArrayInputStream(metaBlock.getData());
		DataInputStream input = new DataInputStream(binput);
		nextMetaBlockIdx = input.readInt();
		int num = input.readInt();
		for (int i = 0; i < num; i++) {
			WritableComparable key = factory.createIndexKey();
			key.read(input);
			BucketID bucket = new BucketID();
			bucket.read(input);
			skipList.add(new Pair<WritableComparable, BucketID>(key, bucket));
		}
	}

	/**
	 * 重置当前cell，用于开始新的写入
	 */
	public void reset() {
		bout.reset();
		try {
			metaDos.writeInt(nextMetaBlockIdx);
			metaDos.writeInt(0);
			lastBuckBout.reset();
			lastBuckMetaDos = new DataOutputStream(lastBuckBout);
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
			setupWriteStream();
			reset();
		}
		code.write(lastBuckMetaDos);
		ByteUtil.writeVInt(lastBuckMetaDos, bOffset);
		ByteUtil.writeVInt(lastBuckMetaDos, octOffset);
		boolean ret = (bout.size() + lastBuckMetaDos.size()) <= Block.availableSpace();
		if (ret) {
			size++;
		} else {
			// 由于需要新加入一个SkipCell，导致当前的bucket的blockID加1,这里将lastbuck中内容更新之后写入到metaDos中去
			DataInputStream input = new DataInputStream(new ByteArrayInputStream(lastBuckBout.toByteArray()));
			WritableComparable tempCode = factory.createIndexKey();
			while (input.available() > 0) {
				tempCode.read(input);
				tempCode.write(metaDos);
				// 当前的bucket的blockID加1
				// TODO:理论上这里应该要+1，但是+1会导致字节数增加
				ByteUtil.writeVInt(metaDos, ByteUtil.readVInt(input));
				ByteUtil.writeVInt(metaDos, ByteUtil.readVInt(input));
			}
			lastBuckBout.reset();
			lastBuckMetaDos.close();
		}
		return ret;
	}

	public boolean addIndex(WritableComparable code, BucketID indexBlockIdx) throws IOException {
		int bOffset = indexBlockIdx.blockID - blockIdx - 1;
		return addIndex(code, bOffset, indexBlockIdx.offset);
	}

	public Block write(int nextMetaBlockIdx) throws IOException {
		if (lastBuckMetaDos.size() > 0) {
			metaDos.write(lastBuckBout.toByteArray());
		}
		this.nextMetaBlockIdx = nextMetaBlockIdx;
		byte[] metaArray = bout.toByteArray();
		ByteUtil.writeInt(nextMetaBlockIdx, metaArray, 0);
		ByteUtil.writeInt(size, metaArray, 4);
		metaBlock = new Block(BLOCKTYPE.META_BLOCK, 0);
		metaBlock.setData(metaArray);
		return metaBlock;
	}

	private void deserialize() throws IOException {
		if (skipList.size() == 0) {
			DataInputStream input = new DataInputStream(new ByteArrayInputStream(bout.toByteArray()));
			input.readInt();
			input.readInt();
			for (int i = 0; i < size; i++) {
				WritableComparable key = factory.createIndexKey();
				key.read(input);
				BucketID bucket = new BucketID();
				bucket.read(input);
				skipList.add(new Pair<WritableComparable, BucketID>(key, bucket));
			}
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		write(-1).write(out);
	}

	@Override
	public void read(DataInput input) throws IOException {
		Block indexBlock = new Block(BLOCKTYPE.META_BLOCK, -1);
		indexBlock.read(input);
		this.read(indexBlock);
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