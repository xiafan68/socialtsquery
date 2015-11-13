package core.io;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.junit.Assert;
import org.junit.Test;

public class BucketTest {
	@Test
	public void test() throws IOException {
		Bucket buck = new Bucket(0);
		buck.storeOctant(new byte[100]);
		buck.storeOctant(new byte[600]);
		ByteArrayOutputStream bOutput = new ByteArrayOutputStream();
		DataOutputStream dos = new DataOutputStream(bOutput);
		buck.write(dos);
		Bucket readBuck = new Bucket(0);
		DataInputStream input = new DataInputStream(new ByteArrayInputStream(bOutput.toByteArray()));
		readBuck.read(input);
		Assert.assertEquals(buck, readBuck);

		System.out.println("----------------------------------------------------");
		buck = new Bucket(0);
		Assert.assertTrue(buck.canStore(3771));
		buck.storeOctant(new byte[3767]);
		Assert.assertTrue(!buck.canStore(391));
		buck.storeOctant(new byte[391]);
		bOutput = new ByteArrayOutputStream();
		dos = new DataOutputStream(bOutput);
		buck.write(dos);
		dos.close();
		readBuck = new Bucket(0);
		input = new DataInputStream(new ByteArrayInputStream(bOutput.toByteArray()));
		readBuck.read(input);
		Assert.assertEquals(buck, readBuck);

		System.out.println("----------------------------------------------------");
		buck = new Bucket(0);
		Assert.assertTrue(buck.canStore(9955));
		buck.storeOctant(new byte[9955]);
		bOutput = new ByteArrayOutputStream();
		dos = new DataOutputStream(bOutput);
		buck.write(dos);
		dos.close();
		readBuck = new Bucket(0);
		input = new DataInputStream(new ByteArrayInputStream(bOutput.toByteArray()));
		readBuck.read(input);
		Assert.assertEquals(buck, readBuck);

		System.out.println("----------------------------------------------------");
		buck = new Bucket(0);
		Assert.assertTrue(buck.canStore(9955));
		buck.storeOctant(new byte[Block.availableSpace() - 4 - 4 - 1]);
		bOutput = new ByteArrayOutputStream();
		dos = new DataOutputStream(bOutput);
		buck.write(dos);
		Assert.assertTrue(buck.singleBlock);
		dos.close();
		readBuck = new Bucket(0);
		input = new DataInputStream(new ByteArrayInputStream(bOutput.toByteArray()));
		readBuck.read(input);
		Assert.assertEquals(buck, readBuck);

		System.out.println("----------------------------------------------------");
		buck = new Bucket(0);
		buck.storeOctant(new byte[10772]);
		bOutput = new ByteArrayOutputStream();
		dos = new DataOutputStream(bOutput);
		buck.write(dos);
		dos.close();
		readBuck = new Bucket(0);
		input = new DataInputStream(new ByteArrayInputStream(bOutput.toByteArray()));
		readBuck.read(input);
		Assert.assertEquals(buck, readBuck);

	}

	@Test
	public void testMultiSmallOcts() throws IOException {
		ByteArrayOutputStream bOutput = new ByteArrayOutputStream();
		DataOutputStream dos = new DataOutputStream(bOutput);
		Bucket buck = new Bucket(0);
		Bucket readBuck = new Bucket(0);
		System.out.println("----------------------------------------------------");
		buck = new Bucket(0);
		for (int i = 0; i < 40; i++) {
			byte[] data = new byte[50];
			Arrays.fill(data, (byte) i);
			buck.storeOctant(data);
		}
		bOutput = new ByteArrayOutputStream();
		dos = new DataOutputStream(bOutput);
		buck.write(dos);
		dos.close();

		readBuck = new Bucket(0);
		DataInputStream input = new DataInputStream(new ByteArrayInputStream(bOutput.toByteArray()));
		readBuck.read(input);
		Assert.assertEquals(buck, readBuck);
	}
}
