package core.index;

import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import core.index.IndexWriter.BlockMeta;

public class IndexWriterTest {
	@Test
	public void test() throws IOException {
		FileInputStream fis = new FileInputStream(
				"/home/xiafan/文档/dataset/output/170092760.idx");
		DataInputStream dis = new DataInputStream(fis);

		byte[] data = new byte[Block.SIZE];
		dis.read(data);
		Block block = new Block(data);
		block.init();
		Assert.assertEquals(Block.HEADER_BLOCK, block.getType());
		int bID = 1;
		int preMetaNum = -1;
		while (dis.available() > 0) {
			int metaBID = bID++;
			long metaOffset = fis.getChannel().position();

			data = new byte[Block.SIZE];
			dis.read(data);
			Block metaBlock = new Block(data);
			metaBlock.init();
			Assert.assertEquals(Block.META_BLOCK, metaBlock.getType());
			DataInputStream bbis = metaBlock.readByStream();
			List<BlockMeta> bMetas = new ArrayList<BlockMeta>();

			Assert.assertEquals(1, metaBID
					% (IndexWriter.numOfMetasPerBlock() + 1));
			Assert.assertEquals(metaBID, metaOffset / Block.SIZE);
			int i = 0;
			if (preMetaNum != -1) {
				Assert.assertEquals(preMetaNum,
						IndexWriter.numOfMetasPerBlock());
			}
			preMetaNum = metaBlock.getRecs();
			for (; i < metaBlock.getRecs(); i++) {
				BlockMeta meta = new BlockMeta(0, 0, 0);
				meta.read(bbis);
				bMetas.add(meta);
				// System.out.println(i + " " + meta);
			}

			for (BlockMeta meta : bMetas) {
				data = new byte[Block.SIZE];
				dis.read(data);
				Block dataBlock = new Block(data);
				Assert.assertEquals(Block.DATA_BLOCK, dataBlock.getType());
				dataBlock.init();
				if (bID == 2527039) {
					System.out.println("bid " + bID + " recs "
							+ dataBlock.getRecs() + " meta bid " + metaBID
							+ " offset " + metaOffset);
				}
				bID++;
				Assert.assertEquals(dataBlock.getRecs(), meta.recNum);
			}
		}

		dis.close();
	}
}
