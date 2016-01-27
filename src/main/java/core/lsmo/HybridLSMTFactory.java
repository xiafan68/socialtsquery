package core.lsmo;

import java.util.List;

import core.lsmi.SortedListMemTable;
import core.lsmo.internformat.BlockBasedSSTableReader;
import core.lsmo.internformat.InternOctreeSSTableWriter;
import core.lsmo.internformat.SortedListToOctreeSSTableWriter;
import core.lsmt.ILSMTFactory;
import core.lsmt.IMemTable;
import core.lsmt.IMemTable.SSTableMeta;
import core.lsmt.ISSTableReader;
import core.lsmt.ISSTableWriter;
import core.lsmt.LSMTInvertedIndex;
import util.Configuration;

/**
 * 内存中采用的是sortedlist的组织方式，磁盘上采用的是octree的方式
 * 
 * @author xiafan
 *
 */
public enum HybridLSMTFactory implements ILSMTFactory {
	INSTANCE;

	@Override
	public ISSTableWriter newSSTableWriterForFlushing(List<IMemTable> tables, Configuration conf) {
		return new SortedListToOctreeSSTableWriter(tables, conf);
	}

	@Override
	public ISSTableWriter newSSTableWriterForCompaction(SSTableMeta meta, List<ISSTableReader> tables,
			Configuration conf) {
		return new InternOctreeSSTableWriter(meta, tables, conf);
	}

	@Override
	public IMemTable newMemTable(LSMTInvertedIndex index, SSTableMeta meta) {
		return new SortedListMemTable(index, meta);
	}

	@Override
	public ISSTableReader newSSTableReader(LSMTInvertedIndex index, SSTableMeta meta) {
		return new BlockBasedSSTableReader(index, meta);
	}

}
