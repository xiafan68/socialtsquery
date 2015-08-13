package common;

import org.junit.Assert;
import org.junit.Test;

import core.commom.Encoding;
import core.commom.Point;

public class EncodingTest {
	@Test
	public void test() {
		Encoding data = new Encoding(new Point(696600, 696602, 2), 0);
		System.out.println(data);
		data.setPaddingBitNum(0);
		data.setX(0);
		System.out.println(data);

		Encoding newCode = new Encoding(new Point(0, 696602, 2), 0);
		System.out.println(newCode);
		Assert.assertTrue(data.compareTo(newCode) == 0);
	}
}