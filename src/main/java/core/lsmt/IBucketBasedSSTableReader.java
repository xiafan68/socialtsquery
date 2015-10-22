package core.lsmt;

import java.io.IOException;

import Util.Pair;
import core.io.Bucket;
import core.io.Bucket.BucketID;

public interface IBucketBasedSSTableReader extends ISSTableReader {
	public int getBucket(BucketID id, Bucket bucket) throws IOException;

	public Pair<WritableComparableKey, BucketID> cellOffset(WritableComparableKey curKey, WritableComparableKey curCode)
			throws IOException;

	public Pair<WritableComparableKey, BucketID> floorOffset(WritableComparableKey curKey,
			WritableComparableKey curCode) throws IOException;
}
