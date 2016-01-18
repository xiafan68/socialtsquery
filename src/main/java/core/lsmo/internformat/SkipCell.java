package core.lsmo.internformat;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import core.io.Block;
import core.io.Block.BLOCKTYPE;
import core.io.Bucket.BucketID;
import core.lsmt.WritableComparableKey;
import core.lsmt.WritableComparableKey.WritableComparableKeyFactory;
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
public class SkipCell {
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
	List<Pair<WritableComparableKey, BucketID>> skipList = new ArrayList<Pair<WritableComparableKey, BucketID>>();

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

	public Pair<WritableComparableKey, BucketID> getIndexEntry(int idx) {
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

	public int cellOffset(WritableComparableKey key, int start, int end) {
		Pair<WritableComparableKey, BucketID> pair = new Pair<WritableComparableKey, BucketID>(key, null);
		int idx = Collections.binarySearch(skipList.subList(start, end), pair,
				new Comparator<Pair<WritableComparableKey, BucketID>>() {
					@Override
					public int compare(Pair<WritableComparableKey, BucketID> o1,
							Pair<WritableComparableKey, BucketID> o2) {
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
			WritableComparableKey key = factory.createIndexKey();
			key.read(input);
			BucketID bucket = new BucketID();
			bucket.read(input);
			skipList.add(new Pair<WritableComparableKey, BucketID>(key, bucket));
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
	private boolean addIndex(WritableComparableKey code, int bOffset, short octOffset) throws IOException {
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
			WritableComparableKey tempCode = factory.createIndexKey();
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

	public boolean addIndex(WritableComparableKey code, BucketID indexBlockIdx) throws IOException {
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
				WritableComparableKey key = factory.createIndexKey();
				key.read(input);
				BucketID bucket = new BucketID();
				bucket.read(input);
				skipList.add(new Pair<WritableComparableKey, BucketID>(key, bucket));
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