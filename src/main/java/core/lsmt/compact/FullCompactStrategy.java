package core.lsmt.compact;

import java.util.ArrayList;
import java.util.List;

import core.lsmt.IMemTable.SSTableMeta;

/**
 * 使用这个策略，磁盘上的所有索引段最终会被压缩为一个索引段
 * @author xiafan
 *
 */
public class FullCompactStrategy extends LSMCompactStrategy {
	@Override
	public List<SSTableMeta> compactFiles(List<SSTableMeta> diskTreeMetas) {
		List<SSTableMeta>  ret = super.compactFiles(diskTreeMetas);
		if (ret.isEmpty()){
			if (levelNums.size()>=2){
				ret = new ArrayList<SSTableMeta>(); 
				int levelA= levelNums.firstKey();
				levelNums.remove(levelA);
				int levelB = levelNums.firstKey();
				ret.add(levelList.get(levelA).get(0));
				ret.add(levelList.get(levelB).get(0));
			}
		}
		return ret;
	}
}
