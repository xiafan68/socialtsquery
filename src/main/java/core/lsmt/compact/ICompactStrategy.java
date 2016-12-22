package core.lsmt.compact;

import java.util.List;

import core.lsmt.IMemTable.SSTableMeta;

/**
 * 合并策略，用于决定哪些文件应该合并到一起
 * @author xiafan
 *
 */
public interface ICompactStrategy {
		
	/**
	 * 返回需要合并的索引段
	 * @param diskFiles
	 * @return
	 */
	public  List<SSTableMeta> compactFiles( List<SSTableMeta> diskFiles);
}
