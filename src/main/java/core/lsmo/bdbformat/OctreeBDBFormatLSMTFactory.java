package core.lsmo.bdbformat;

import java.util.List;

import core.lsmo.OctreeMemTable;
import core.lsmt.ILSMTFactory;
import core.lsmt.IMemTable;
import core.lsmt.IMemTable.SSTableMeta;
import core.lsmt.LSMTInvertedIndex;
import core.lsmt.postinglist.ISSTableReader;
import core.lsmt.postinglist.ISSTableWriter;
import util.Configuration;

public enum OctreeBDBFormatLSMTFactory implements ILSMTFactory {
	INSTANCE;

	@SuppressWarnings("rawtypes")
	@Override
	public ISSTableWriter newSSTableWriterForFlushing(List<IMemTable> tables, Configuration conf) {
		return new BDBSkipListOctreeSSTableWriter(tables, conf);
	}

	@Override
	public ISSTableWriter newSSTableWriterForCompaction(SSTableMeta meta, List<ISSTableReader> tables,
			Configuration conf) {
		return new BDBSkipListOctreeSSTableWriter(meta, tables, conf);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public IMemTable newMemTable(LSMTInvertedIndex index, SSTableMeta meta) {
		return new OctreeMemTable(index, meta);
	}

	@Override
	public ISSTableReader newSSTableReader(LSMTInvertedIndex index, SSTableMeta meta) {
		return new DiskSSTableBDBReader(index, meta);
	}

}
