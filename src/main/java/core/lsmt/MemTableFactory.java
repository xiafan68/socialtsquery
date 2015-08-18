package core.lsmt;

import core.lsmo.OctreeMemTable;
import core.lsmt.IMemTable.SSTableMeta;

public class MemTableFactory {
	public static IMemTable newMemTable(LSMOInvertedIndex index, SSTableMeta meta) {
		return new OctreeMemTable(index, meta);
	}
}
