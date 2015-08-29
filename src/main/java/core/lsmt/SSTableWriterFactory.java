package core.lsmt;

import java.util.List;

import core.lsmo.OctreeMemTable;
import core.lsmo.OctreeSSTableWriter;
import core.lsmt.IMemTable.SSTableMeta;

/**
 * 创建sstablewriter的工厂类
 * 
 * @author xiafan
 *
 */
public enum SSTableWriterFactory {
	INSTANCE;

	public ISSTableWriter newWriterForFlushing(List<IMemTable> tables, int step) {
		return new OctreeSSTableWriter(tables, step);
	}

	public ISSTableWriter newWriterForCompaction(SSTableMeta meta, List<ISSTableReader> tables, int step) {
		return new OctreeSSTableWriter(meta, tables, step);
	}
}
