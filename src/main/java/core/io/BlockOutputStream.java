package core.io;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import core.io.Block.BLOCKTYPE;

/**
 * use this class to facilitate sequential writing of data blocks where
 * potential rewrite to blocks in appended blocks is possible.
 * 
 * @author xiafan
 *
 */
public class BlockOutputStream {

	FileOutputStream dataFileOs;
	DataOutputStream dataDos;

	Map<Integer, Integer> blockIdxMapping = new HashMap<Integer, Integer>();
	List<Block> appendBlocks = new ArrayList<Block>();
	int appendSize;

	public BlockOutputStream(File filePath) {
		try {
			dataFileOs = new FileOutputStream(filePath);
			dataDos = new DataOutputStream(dataFileOs);
		} catch (FileNotFoundException e) {
			throw new RuntimeException(e);
		}
	}

	public int currentBlockIdx() throws IOException {
		return (int) ((dataFileOs.getChannel().position() + appendSize) / Block.BLOCK_SIZE);
	}

	public Block newBlock(BLOCKTYPE type) throws IOException {
		Block block = new Block(type, currentBlockIdx());
		return block;
	}

	public void appendBlock(Block block) throws IOException {
		block.setBlockIdx(currentBlockIdx());
		appendSize += Block.BLOCK_SIZE;
		appendBlocks.add(block);
		blockIdxMapping.put(block.getBlockIdx(), appendBlocks.size() - 1);
	}

	public void appendBlocks(List<Block> blocks) throws IOException {
		for (Block block : blocks) {
			appendBlock(block);
		}
	}

	public void writeBlock(Block block) throws IOException {
		if (blockIdxMapping.containsKey(block.getBlockIdx())) {
			appendBlocks.set(blockIdxMapping.get(block.getBlockIdx()), block);
		} else {
			// in such case we think you are appending to the end
			block.write(dataDos);
		}
	}

	public void writeBlocks(List<Block> blocks) throws IOException {
		for (Block block : blocks) {
			writeBlock(block);
		}

	}

	public void flushAppends() throws IOException {
		for (Block block : appendBlocks) {
			block.write(dataDos);
		}
		appendBlocks.clear();
		blockIdxMapping.clear();
	}

	public void close() throws IOException {
		dataDos.close();
		dataFileOs.close();
	}
}
