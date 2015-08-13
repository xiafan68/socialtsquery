package core.lsmo.octree;

import Util.Pair;

/**
 * 只访问和[start,end]有交集的octants TODO: 按照Z的value，从大到小，返回每个Z对应的IOctreeIterator
 * 
 * @author xiafan
 * 
 */
public abstract class IOctreeRangeIterator {

	public abstract boolean hasNext();

	public abstract Pair<Integer, IOctreeIterator> next();
}
