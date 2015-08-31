package core.lsmt;

import java.util.List;

import core.lsmt.IMemTable.SSTableMeta;

/**
 * factory used to define different implementations for different components
 * @author xiafan
 *
 */
public interface ILSMTFactory {

	// interfaces for the writer
	public ISSTableWriter newSSTableWriterForFlushing(List<IMemTable> tables,
			int step);

	public ISSTableWriter newSSTableWriterForCompaction(SSTableMeta meta,
			List<ISSTableReader> tables, int step);

	// interface for the memtable
	public IMemTable newMemTable(LSMTInvertedIndex index, SSTableMeta meta);

	public ISSTableReader newSSTableReader(LSMTInvertedIndex index,
			SSTableMeta meta);
}
