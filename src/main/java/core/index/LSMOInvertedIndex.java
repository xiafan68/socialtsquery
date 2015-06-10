package core.index;

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 
 * @author xiafan
 *
 */
public class LSMOInvertedIndex {
	private Map<String, LogStructureOctree> dir;

	AtomicBoolean running = new AtomicBoolean(true);

	public LogStructureOctree getPostingList(String word) {
		LogStructureOctree ret = dir.get(word);
		if (!ret.increRef()) {
			ret = null;
		}
		return ret;
	}

	public void release(LogStructureOctree tree) {

	}

}
