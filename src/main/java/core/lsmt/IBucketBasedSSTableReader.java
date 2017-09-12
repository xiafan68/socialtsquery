package core.lsmt;

import java.io.IOException;

import core.io.Bucket;
import core.io.Bucket.BucketID;

public interface IBucketBasedSSTableReader extends ISSTableReader {
	public int getBucket(BucketID id, Bucket bucket) throws IOException;

	public BucketID cellOffset(WritableComparable curKey, WritableComparable curCode) throws IOException;

	public BucketID floorOffset(WritableComparable curKey, WritableComparable curCode) throws IOException;
}
