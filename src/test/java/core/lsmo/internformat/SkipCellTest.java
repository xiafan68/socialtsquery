package core.lsmo.internformat;

import java.io.IOException;

import org.junit.Assert;
import org.junit.Test;

import core.commom.Encoding;
import core.commom.WritableComparable.EncodingFactory;
import core.io.Block;
import core.io.Bucket.BucketID;
import core.lsmo.persistence.SkipCell;

public class SkipCellTest {
	@Test
	public void test() throws IOException {
		SkipCell cell = new SkipCell(0, EncodingFactory.INSTANCE);
		cell.reset();
		for (int i = 0; i < 10; i++) {
			for (int j = 0; j < 10; j++) {
				cell.addIndex(new Encoding(), new BucketID(i, (short) j));
			}
		}
		Block block = cell.write(1);
		SkipCell newCell = new SkipCell(0, EncodingFactory.INSTANCE);
		newCell.reset();
		newCell.read(block);
		Assert.assertTrue(cell.equals(newCell));

		cell.reset();
		boolean stop = false;
		for (int i = 0; i < 50 && !stop; i++) {
			for (int j = 0; j < 10; j++) {
				if (!cell.addIndex(new Encoding(), new BucketID(i, (short) j))) {
					stop = true;
					break;
				}
			}
		}
		block = cell.write(1);
		newCell = new SkipCell(0, EncodingFactory.INSTANCE);
		newCell.reset();
		newCell.read(block);
		Assert.assertTrue(cell.equals(newCell));
	}
}
