package core.lsmt;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;

import Util.Configuration;
import core.lsmo.MemTable;
import core.lsmo.SSTableWriter;
import core.lsmt.IMemTable.SSTableMeta;

/**
 * responsible for write data to disk
 * 
 * @author xiafan
 *
 */
public class FlushService extends Thread {
	private static final Logger logger = Logger.getLogger(FlushService.class);
	LSMOInvertedIndex index;
	Configuration conf;

	public FlushService(LSMOInvertedIndex index) {
		super("flush thread");
		this.index = index;
		conf = index.getConf();
	}

	@Override
	public void run() {
		while (!index.stop) {
			LockManager.INSTANCE.versionReadLock();
			List<IMemTable> flushingTables = new ArrayList<IMemTable>(index.getVersion().flushingTables);
			LockManager.INSTANCE.versionReadUnLock();
			if (!flushingTables.isEmpty()) {
				StringBuffer buf = new StringBuffer("flushing versions ");
				for (IMemTable memTable : flushingTables.subList(0, 1)) {
					buf.append(memTable.getMeta().version);
					buf.append(" n:");
					buf.append(memTable.size() + " ");
				}
				logger.info(buf.toString());

				ISSTableWriter writer = SSTableWriterFactory.INSTANCE.newWriterForFlushing(flushingTables.subList(0, 1),
						index.getStep());
				try {
					writer.write(conf.getTmpDir());
					writer.close();

					// write succeed, now move file to the right place, update
					// the versionset and commitlog
					File tmpFile = SSTableWriter.idxFile(conf.getTmpDir(), writer.getMeta());
					tmpFile.renameTo(SSTableWriter.idxFile(conf.getIndexDir(), writer.getMeta()));
					tmpFile = SSTableWriter.dirMetaFile(conf.getTmpDir(), writer.getMeta());
					tmpFile.renameTo(SSTableWriter.dirMetaFile(conf.getIndexDir(), writer.getMeta()));
					tmpFile = SSTableWriter.dataFile(conf.getTmpDir(), writer.getMeta());
					tmpFile.renameTo(SSTableWriter.dataFile(conf.getIndexDir(), writer.getMeta()));
					Set<SSTableMeta> versions = new HashSet<SSTableMeta>();
					for (IMemTable table : flushingTables) {
						versions.add(table.getMeta());
					}
					index.flushTables(versions, writer.getMeta());
				} catch (IOException e) {
					throw new RuntimeException(e);
				}
			} else {
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
	}
}