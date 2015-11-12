package common;

import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.junit.Assert;
import org.junit.Test;

import core.commom.Encoding;
import core.commom.Point;

public class EncodingTest {
	@Test
	public void test() throws IOException {
		Encoding data = new Encoding(new Point(696600, 696602, 2), 0);
		System.out.println(data);
		data.setPaddingBitNum(0);
		data.setX(0);
		System.out.println(data);

		Encoding newCode = new Encoding(new Point(0, 696602, 2), 0);
		System.out.println(newCode);
		Assert.assertTrue(data.compareTo(newCode) == 0);

		ByteArrayOutputStream tempDataBout = new ByteArrayOutputStream();
		DataOutputStream tempDataDos = new DataOutputStream(tempDataBout);
		for (int i = 0; i < 1000; i++) {
			tempDataDos.writeInt(i);
		}
		int preSize = tempDataBout.toByteArray().length;
		tempDataBout.reset();
		tempDataDos = new DataOutputStream(tempDataBout);
		for (int i = 0; i < 100; i++) {
			tempDataDos.writeInt(i);
		}
		System.out.println(preSize + "," + tempDataBout.toByteArray().length);

	}

	@Test
	public void removeDirTest() {
		try {
			FileUtils.deleteDirectory(new File("/home/xiafan/temp/lsmo_twitter/data/362_0_dir"));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
