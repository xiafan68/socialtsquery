package core.lsmo.bdbformat;

import java.util.List;

import core.lsmo.OctreeMemTable;
import core.lsmt.ILSMTFactory;
import core.lsmt.IMemTable;
import core.lsmt.IMemTable.SSTableMeta;
import core.lsmt.postinglist.ISSTableReader;
import core.lsmt.postinglist.ISSTableWriter;
import core.lsmt.LSMTInvertedIndex;
import util.Configuration;

public enum OctreeBDBFormatLSMTFactory implements ILSMTFactory {
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
