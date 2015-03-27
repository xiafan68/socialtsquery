package core.index;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import junit.framework.Assert;

import org.junit.Test;

public class BlockTest {
	@Test
	public void serializeTest() throws IOException {
		Block b = new Block();
		b.cur = 8;
		b.recs = 10010;
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		DataOutputStream dos = new DataOutputStream(bos);
		b.write(dos);
		Block a = new Block(bos.toByteArray());
		Assert.assertEquals(b.cur, a.cur);
		Assert.assertEquals(b.recs, a.recs);
	}
}
