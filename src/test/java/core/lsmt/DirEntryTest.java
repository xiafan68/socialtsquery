package core.lsmt;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.junit.Test;

import core.commom.WritableComparable.StringKey;

public class DirEntryTest {
	@Test
	public void test() throws IOException {
		DirEntry entry = new DirEntry();
		entry.curKey = new StringKey("test");
		entry.startBucketID.blockID = 2650;
		entry.startBucketID.offset = 25;
		entry.endBucketID.blockID = 2730;
		entry.endBucketID.offset = 5;

		entry.indexStartOffset = 11351598563471l;
		entry.sampleNum = 11351598563479l;
		ByteArrayOutputStream bOutput = new ByteArrayOutputStream();
		DataOutputStream dos = new DataOutputStream(bOutput);
		entry.write(dos);
		DataInputStream input = new DataInputStream(new ByteArrayInputStream(bOutput.toByteArray()));
		DirEntry newEntry = new DirEntry();
		newEntry.read(input);
		System.out.println(entry + "\n" + newEntry);
		// Assert.assertTrue(entry.equals(newEntry));
	}

}
