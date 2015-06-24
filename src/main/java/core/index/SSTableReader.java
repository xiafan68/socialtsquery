package core.index;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.HashMap;
import java.util.Map;

import core.commom.Encoding;
import core.index.MemTable.SSTableMeta;
import core.index.SSTableWriter.DirEntry;
import core.index.octree.DiskOctreeIterator;
import core.index.octree.IOctreeIterator;
import core.io.Bucket;
import core.io.Bucket.BucketID;

public class SSTableReader {
	LSMOInvertedIndex index;
	SSTableMeta meta;
	RandomAccessFile dataInput;
	RandomAccessFile dirInput;
	RandomAccessFile indexInput;

	Map<Integer, DirEntry> entries = new HashMap<Integer, DirEntry>();

	public SSTableReader(LSMOInvertedIndex index, SSTableMeta meta) {

	}

	public IOctreeIterator getPostingListScanner(int key) {
		return new DiskOctreeIterator(entries.get(key), this);
	}

	public Bucket getBucket(BucketID id) throws IOException {
		dataInput.seek(id.getFileOffset());
		Bucket bucket = new Bucket(0);
		bucket.read(dataInput);
		return bucket;
	}

	public Map<Encoding, BucketID> getIndex(int key) throws IOException {
		Map<Encoding, BucketID> ret = new HashMap<Encoding, BucketID>();
		DirEntry entry = entries.get(key);
		indexInput.seek(entry.getIndexOffset());
		for (int i = 0; i < entry.sampleNum; i++) {
			Encoding encoding = new Encoding();
			encoding.readFields(indexInput);
			BucketID id = new BucketID();
			id.read(indexInput);
			ret.put(encoding, id);
		}
		return ret;
	}
}
