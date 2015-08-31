package core.lsmo;

import java.util.List;

import core.lsmt.IMemTable;
import core.lsmt.IMemTable.SSTableMeta;
import core.lsmt.ISSTableReader;
import core.lsmt.ISSTableWriter;
import core.lsmt.ILSMTFactory;
import core.lsmt.LSMTInvertedIndex;

public enum OctreeBasedLSMTFactory implements ILSMTFactory {
	INSTANCE;
	@Override
	public ISSTableWriter newSSTableWriterForFlushing(List<IMemTable> tables,
			int step) {
		return new OctreeSSTableWriter(tables, step);
	}

	@Override
	public ISSTableWriter newSSTableWriterForCompaction(SSTableMeta meta,
			List<ISSTableReader> tables, int step) {
		return new OctreeSSTableWriter(meta, tables, step);
	}

	@Override
	public IMemTable newMemTable(LSMTInvertedIndex index, SSTableMeta meta) {
		return new OctreeMemTable(index, meta);
	}

	@Override
	public ISSTableReader newSSTableReader(LSMTInvertedIndex index,
			SSTableMeta meta) {
		return new DiskSSTableReader(index, meta);
	}

}
