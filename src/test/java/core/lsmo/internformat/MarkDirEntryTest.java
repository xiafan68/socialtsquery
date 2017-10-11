package core.lsmo.internformat;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.junit.Assert;
import org.junit.Test;

import core.lsmo.MarkDirEntry;

public class MarkDirEntryTest {
	@Test
	public void test() throws IOException {
		MarkDirEntry entry = new MarkDirEntry();
		entry.startBucketID.blockID = 1;
		entry.startBucketID.offset = 1;
		entry.endBucketID.blockID = 1;
		entry.endBucketID.offset = 1;
		entry.maxTime = 0;
		entry.minTime = 1;

		ByteArrayOutputStream out = new ByteArrayOutputStream();
		DataOutputStream dos = new DataOutputStream(out);
		entry.write(dos);

		DataInputStream input = new DataInputStream(new ByteArrayInputStream(out.toByteArray()));
		MarkDirEntry another = new MarkDirEntry();
		another.read(input);
		Assert.assertTrue(entry.equals(another));
		System.out.println(another);
	}
}
