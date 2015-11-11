package core.lsmo.internformat;

import java.util.List;

import Util.Configuration;
import core.lsmo.OctreeMemTable;
import core.lsmt.ILSMTFactory;
import core.lsmt.IMemTable;
import core.lsmt.IMemTable.SSTableMeta;
import core.lsmt.ISSTableReader;
import core.lsmt.ISSTableWriter;
import core.lsmt.LSMTInvertedIndex;

public enum OctreeInternFormatLSMTFactory implements ILSMTFactory {
	INSTANCE;
	@Override
	public ISSTableWriter newSSTableWriterForFlushing(List<IMemTable> tables, Configuration conf) {
		return new InternOctreeSSTableWriter(tables, conf);
	}

	@Override
	public ISSTableWriter newSSTableWriterForCompaction(SSTableMeta meta, List<ISSTableReader> tables,
			Configuration conf) {
		return new InternOctreeSSTableWriter(meta, tables, conf);
	}

	@Override
	public IMemTable newMemTable(LSMTInvertedIndex index, SSTableMeta meta) {
		return new OctreeMemTable(index, meta);
	}

	@Override
	public ISSTableReader newSSTableReader(LSMTInvertedIndex index, SSTableMeta meta) {
		return new BlockBasedSSTableReader(index, meta);
	}

}
