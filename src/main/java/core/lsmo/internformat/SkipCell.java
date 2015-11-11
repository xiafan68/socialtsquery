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

import Util.Pair;
import core.io.Block;
import core.io.Block.BLOCKTYPE;
import core.io.Bucket.BucketID;
import core.lsmt.WritableComparableKey;
import core.lsmt.WritableComparableKey.WritableComparableKeyFactory;
import fanxia.file.ByteUtil;

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
	ByteArrayOutputStream bout = new ByteArrayOutputStream(Block.availableSpace());
	DataOutputStream metaDos = new DataOutputStream(bout);
	List<Pair<WritableComparableKey, BucketID>> skipList = new ArrayList<Pair<WritableComparableKey, BucketID>>();
	int size = 0;
	int nextMetaBlockIdx = 0;// 下一个meta block的地址

	public SkipCell(int blockIdx) {
		this.blockIdx = blockIdx;
	}

	public int getBlockIdx() {
		return blockIdx;
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

	public void read(Block metaBlock, WritableComparableKeyFactory factory) throws IOException {
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

	public void reset() {
		bout.reset();
		try {
			metaDos.writeInt(nextMetaBlockIdx);
			metaDos.writeInt(0);
			size = 0;
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
	public boolean addIndex(WritableComparableKey code, int bOffset, short octOffset) throws IOException {
		code.write(metaDos);
		ByteUtil.writeVInt(metaDos, bOffset);
		ByteUtil.writeVInt(metaDos, octOffset);
		boolean ret = bout.size() <= Block.availableSpace();
		if (ret) {
			size++;
		}
		return ret;
	}

	public Block write(int nextMetaBlockIdx) throws IOException {
		byte[] metaArray = bout.toByteArray();
		ByteUtil.writeInt(size, metaArray, 4);
		metaBlock = new Block(BLOCKTYPE.META_BLOCK, 0);
		metaBlock.setData(metaArray);
		return metaBlock;
	}

}