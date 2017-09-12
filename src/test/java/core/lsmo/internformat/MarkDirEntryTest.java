package core.lsmo.internformat;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.junit.Test;

import core.lsmo.internformat.InternOctreeSSTableWriter.MarkDirEntry;
import core.lsmt.WritableComparable.StringKey;
import core.lsmt.WritableComparable.StringKeyFactory;

public class MarkDirEntryTest {
	@Test
	public void test() throws IOException {
		MarkDirEntry entry = new MarkDirEntry(StringKeyFactory.INSTANCE);
		entry.curKey = new StringKey("sfd");
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		DataOutputStream dos = new DataOutputStream(out);
		entry.write(dos);

		DataInputStream input = new DataInputStream(new ByteArrayInputStream(out.toByteArray()));
		MarkDirEntry another = new MarkDirEntry(StringKeyFactory.INSTANCE);
		another.read(input);
		System.out.println(another);
	}
}
