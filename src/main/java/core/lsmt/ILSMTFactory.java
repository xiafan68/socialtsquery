package core.lsmt;

import java.util.List;

import core.lsmt.IMemTable.SSTableMeta;
import util.Configuration;

/**
 * factory used to define different implementations for different components
 * 
 * @author xiafan
 *
 */
public interface ILSMTFactory {

	// interfaces for the writer
	public ISSTableWriter newSSTableWriterForFlushing(List<IMemTable> tables, Configuration conf);

	public ISSTableWriter newSSTableWriterForCompaction(SSTableMeta meta, List<ISSTableReader> tables,
			Configuration conf);

	// interface for the memtable
	public IMemTable newMemTable(LSMTInvertedIndex index, SSTableMeta meta);

	public ISSTableReader newSSTableReader(LSMTInvertedIndex index, SSTableMeta meta);
}
