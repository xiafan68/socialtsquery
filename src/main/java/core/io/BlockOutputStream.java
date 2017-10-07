package core.io;

import java.io.DataOutputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;

import core.io.Block.BLOCKTYPE;

public class BlockOutputStream {

	FileOutputStream dataFileOs;
	DataOutputStream dataDos;

	List<Block> appendBlocks;
	int appendSize;

	public BlockOutputStream(String filePath) {
		try {
			dataFileOs = new FileOutputStream(filePath);
			dataDos = new DataOutputStream(dataFileOs);
		} catch (FileNotFoundException e) {
			throw new RuntimeException(e);
		}
	}

	public int currentBlockIdx() throws IOException{
		return (int) ((dataFileOs.getChannel().position()+appendSize)/ Block.BLOCK_SIZE);
	}
	
	public Block newBlock(BLOCKTYPE type) throws IOException {
		Block block = new Block(type, currentBlockIdx());
		return block;
	}
	
	public void appendBlock(Block block){
		appendSize += Block.BLOCK_SIZE;
		appendBlocks.add(block);
	}
	
	public void flushAppends() throws IOException{
		for(Block block : appendBlocks){
			block.write(dataDos);
		}
	}
	
	public void close() throws IOException{
		dataDos.close();
		dataFileOs.close();
	}
}
