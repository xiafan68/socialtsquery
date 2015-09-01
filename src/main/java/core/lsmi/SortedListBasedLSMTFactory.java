package core.lsmi;

import java.util.List;

import Util.Configuration;
import core.lsmt.IMemTable;
import core.lsmt.IMemTable.SSTableMeta;
import core.lsmt.ISSTableReader;
import core.lsmt.ISSTableWriter;
import core.lsmt.ILSMTFactory;
import core.lsmt.LSMTInvertedIndex;

public enum SortedListBasedLSMTFactory implements ILSMTFactory {
	INSTANCE;
	@Override
	public ISSTableWriter newSSTableWriterForFlushing(List<IMemTable> tables, Configuration conf) {
		return new SortedListSSTableWriter(tables, conf);
	}

	@Override
	public ISSTableWriter newSSTableWriterForCompaction(SSTableMeta meta, List<ISSTableReader> tables,
			Configuration conf) {
		return new SortedListSSTableWriter(meta, tables, conf);
	}

	@Override
	public IMemTable newMemTable(LSMTInvertedIndex index, SSTableMeta meta) {
		return new SortedListMemTable(index, meta);
	}

	@Override
	public ISSTableReader newSSTableReader(LSMTInvertedIndex index, SSTableMeta meta) {
		return new ListDiskSSTableReader(index, meta);
	}

}
