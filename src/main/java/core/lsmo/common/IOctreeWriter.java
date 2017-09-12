package core.lsmo.common;

import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.log4j.Logger;

import core.commom.Encoding;
import core.io.Bucket;
import core.io.Bucket.BucketID;
import core.lsmo.internformat.InternOctreeSSTableWriter.MarkDirEntry;
import core.lsmo.octree.OctreeNode;
import core.lsmt.IMemTable.SSTableMeta;
import core.lsmt.ISSTableWriter.DirEntry;
import core.lsmt.IPostingListerWriter;
import core.lsmt.WritableComparable;

public abstract class IOctreeWriter extends IPostingListerWriter {
	private static final Logger logger = Logger.getLogger(IOctreeWriter.class);
	protected SkipCell cell;
	protected Bucket markUpBuck; // the bucket used to store (key, offset) pairs
	protected Bucket dataBuck; // the bucket that stores octants

	protected int curStep = 0;
	protected boolean writeFirstBlock = true;
	protected boolean sampleFirstIndex = true;
	protected boolean writeFirstMark = true;
	protected OctreeSSTableWriter writer;

	public IOctreeWriter(OctreeSSTableWriter writer) {
		super(writer.getConf());
		this.writer = writer;
	}

	public abstract int currentMarkIdx() throws IOException;

	@Override
	public void startPostingList(WritableComparable key, BucketID newListStart) throws IOException {
		curDir = new MarkDirEntry(conf.getIndexKeyFactory());
		curDir.curKey = key;
		writeFirstBlock = true;
		sampleFirstIndex = true;
		writeFirstMark = true;
	}

	/**
	 * 当当前posting list第一次存入了dataBuck时才调用这个函数
	 * 
	 * @throws IOException
	 */
	public abstract void firstDataBuckWritten() throws IOException;

	@Override
	public DirEntry endPostingList(BucketID postingListEnd) throws IOException {
		curDir.endBucketID.copy(dataBuck.blockIdx());
		curDir.sampleNum = cell.toFileOffset();
		return curDir;
	}

	public abstract void flushAndNewSkipCell() throws IOException;

	public abstract void flushAndNewDataBuck() throws IOException;

	public abstract void flushAndNewMarkBuck() throws IOException;

	public void storeSentinelOctant(Encoding encoding, byte[] data) throws IOException {
		if (!markUpBuck.canStore(data.length)) {
			flushAndNewMarkBuck();
		}
		markUpBuck.storeOctant(data);
		if (writeFirstMark) {
			((MarkDirEntry) curDir).startMarkOffset.copy(markUpBuck.blockIdx());
			writeFirstMark = false;
		}
		((MarkDirEntry) curDir).markNum++;
	}

	public void storeDataOctant(Encoding encoding, byte[] data) throws IOException {
		if (!dataBuck.canStore(data.length)) {
			flushAndNewDataBuck();
		}
		dataBuck.storeOctant(data);

		if (writeFirstBlock) {
			this.firstDataBuckWritten();
		}

		// sample index
		if (curStep++ % step == 0) {
			buildIndex(encoding, null);
		}
	}

	@Override
	public void buildIndex(WritableComparable code, BucketID id) throws IOException {
		if (!cell.addIndex(code, dataBuck.blockIdx())) {
			flushAndNewSkipCell();
			cell.addIndex(code, dataBuck.blockIdx());
		}
		if (sampleFirstIndex) {
			curDir.indexStartOffset = cell.toFileOffset();
			sampleFirstIndex = false;
		}
	}

	public void addOctant(OctreeNode node) throws IOException {
		// store the current node
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		DataOutputStream dos = new DataOutputStream(baos);
		// first write the octant code, then write the octant
		logger.debug(node.getEncoding());
		node.getEncoding().write(dos);
		node.write(dos);
		byte[] data = baos.toByteArray();
		if (node.size() == 0) {
			storeSentinelOctant(node.getEncoding(), data);
		} else {
			storeDataOctant(node.getEncoding(), data);
		}
	}
	
	@Override
	public boolean validate(SSTableMeta meta) {
		return true;
	}
}