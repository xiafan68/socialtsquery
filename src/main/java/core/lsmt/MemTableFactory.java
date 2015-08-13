package core.lsmt;

import core.lsmo.MemTable;
import core.lsmt.IMemTable.SSTableMeta;

public class MemTableFactory {
	public static IMemTable newMemTable(LSMOInvertedIndex index, SSTableMeta meta) {
		return new MemTable(index, meta);
	}
}