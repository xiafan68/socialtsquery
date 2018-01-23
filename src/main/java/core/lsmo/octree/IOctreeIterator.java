package core.lsmo.octree;

import java.io.IOException;

import core.lsmt.postinglist.IPostingListIterator;

public interface IOctreeIterator extends IPostingListIterator {

	public void addNode(OctreeNode node);

	public void open() throws IOException;

	public boolean hasNext() throws IOException;

	public OctreeNode nextNode() throws IOException;

	public void close() throws IOException;
}
