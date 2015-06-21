package core.index.octree;

import java.io.IOException;

public interface IOctreeIterator {
	public void addNode(OctreeNode node);

	public void open() throws IOException;

	public boolean hasNext() throws IOException;

	public OctreeNode next() throws IOException;

	public void close() throws IOException;
}
