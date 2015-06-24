package core.index.octree;

import java.io.IOException;

import core.index.octree.MemoryOctree.OctreeMeta;

public interface IOctreeIterator {

	public OctreeMeta getMeta();

	public void addNode(OctreeNode node);

	public void open() throws IOException;

	public boolean hasNext() throws IOException;

	public OctreeNode next() throws IOException;

	public void close() throws IOException;
}
