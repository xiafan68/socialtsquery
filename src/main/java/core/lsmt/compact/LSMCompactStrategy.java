package core.lsmt.compact;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.Map.Entry;

import core.lsmt.IMemTable.SSTableMeta;

/**
 * 当前策略首先统计每个层级待压缩的文件数，然后返回层级文件数最多的那个层级，用于进行压缩
 * 
 * @author xiafan
 *
 */
public class LSMCompactStrategy implements ICompactStrategy {

	// 统计每个层级的索引段数
	protected TreeMap<Integer, Integer> levelNums = new TreeMap<Integer, Integer>();
	// 保存每个层级对应的索引段
	protected Map<Integer, List<SSTableMeta>> levelList = new HashMap<Integer, List<SSTableMeta>>();

	@Override
	public List<SSTableMeta> compactFiles(List<SSTableMeta> diskTreeMetas) {
		levelNums = new TreeMap<Integer, Integer>();
		levelList = new HashMap<Integer, List<SSTableMeta>>();

		List<SSTableMeta> ret = new ArrayList<SSTableMeta>();
		for (SSTableMeta meta : diskTreeMetas) {
			if (levelNums.containsKey(meta.level)) {
				levelList.get(meta.level).add(meta);
				levelNums.put(meta.level, levelNums.get(meta.level) + 1);
			} else {
				levelNums.put(meta.level, 1);
				ArrayList<SSTableMeta> metas = new ArrayList<SSTableMeta>();
				metas.add(meta);
				levelList.put(meta.level, metas);
			}
		}
		ArrayList<Entry<Integer, Integer>> sortedLevelNum = new ArrayList<Entry<Integer, Integer>>(
				levelNums.entrySet());
		if (sortedLevelNum.isEmpty()) {
			return ret;
		}

		Collections.sort(sortedLevelNum, new Comparator<Entry<Integer, Integer>>() {
			@Override
			public int compare(Entry<Integer, Integer> o1, Entry<Integer, Integer> o2) {
				return o2.getValue().compareTo(o1.getValue());
			}
		});

		ret = levelList.get(sortedLevelNum.get(0).getKey());
		if (ret.size() >= 2)
			return ret;
		else {
			return new ArrayList<SSTableMeta>();
		}
	}
}
