package core.io;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.commons.io.output.ByteArrayOutputStream;
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

		buck = new Bucket(0);
		buck.storeOctant(new byte[4096]);
		buck.storeOctant(new byte[600]);
		bOutput = new ByteArrayOutputStream();
		dos = new DataOutputStream(bOutput);
		buck.write(dos);

		readBuck = new Bucket(0);
		input = new DataInputStream(new ByteArrayInputStream(bOutput.toByteArray()));
		readBuck.read(input);

	}
}
