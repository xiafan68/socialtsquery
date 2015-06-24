package core.index;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import Util.Configuration;

/**
 * responsible for write data to disk
 * @author xiafan
 *
 */
public class FlushService extends Thread {
	LSMOInvertedIndex index;
	Configuration conf;

	public FlushService(LSMOInvertedIndex index) {
		super("flush thread");
		this.index = index;
		conf = index.getConf();
	}

	@Override
	public void run() {
		while (index.running.get()) {
			LockManager.instance.versionReadLock();
			List<MemTable> flushingTables = new ArrayList<MemTable>(
					index.getVersion().flushingTables);
			LockManager.instance.versionReadUnLock();
			SSTableWriter writer = new SSTableWriter(flushingTables,
					index.getStep());
			try {
				writer.write(conf.getTmpDir());
				writer.close();
				// write succeed, now move file to the right place, update the
				// versionset and commitlog

				File tmpFile = SSTableWriter.idxFile(conf.getTmpDir(),
						writer.getMeta());
				tmpFile.renameTo(SSTableWriter.idxFile(conf.getIndexDir(),
						writer.getMeta()));
				tmpFile = SSTableWriter.dirMetaFile(conf.getTmpDir(),
						writer.getMeta());
				tmpFile.renameTo(SSTableWriter.dirMetaFile(conf.getIndexDir(),
						writer.getMeta()));
				tmpFile = SSTableWriter.dataFile(conf.getTmpDir(),
						writer.getMeta());
				tmpFile.renameTo(SSTableWriter.dataFile(conf.getIndexDir(),
						writer.getMeta()));
				Set<Integer> versions = new HashSet<Integer>();
				for (MemTable table : flushingTables) {
					versions.add(table.getMeta().version);
				}
				index.flushTables(versions, writer.getMeta());
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}
	}
}
