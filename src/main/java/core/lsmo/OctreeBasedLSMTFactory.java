package core.lsmo;

import java.util.List;

import core.lsmo.bdbformat.DiskSSTableBDBReader;
import core.lsmo.bdbformat.OctreeSSTableWriter;
import core.lsmt.ILSMTFactory;
import core.lsmt.IMemTable;
import core.lsmt.IMemTable.SSTableMeta;
import core.lsmt.LSMTInvertedIndex;
import core.lsmt.postinglist.ISSTableReader;
import core.lsmt.postinglist.ISSTableWriter;
import util.Configuration;

/**
 * 这里面
 * @author xiafan
 *
 */
public enum OctreeBasedLSMTFactory implements ILSMTFactory {
	INSTANCE;
	@Override
	public ISSTableWriter newSSTableWriterForFlushing(List<IMemTable> tables, Configuration conf) {
		return new OctreeSSTableWriter(tables, conf);
	}

	@Override
	public ISSTableWriter newSSTableWriterForCompaction(SSTableMeta meta, List<ISSTableReader> tables,
			Configuration conf) {
		return new OctreeSSTableWriter(meta, tables, conf);
	}

	@Override
	public IMemTable newMemTable(LSMTInvertedIndex index, SSTableMeta meta) {
		return new OctreeMemTable(index, meta);
	}

	@Override
	public ISSTableReader newSSTableReader(LSMTInvertedIndex index, SSTableMeta meta) {
		return new DiskSSTableBDBReader(index, meta);
	}

}
