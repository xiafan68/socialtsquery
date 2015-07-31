package core.index;

import java.io.IOException;

import org.junit.Test;

import segmentation.Segment;
import Util.Configuration;
import common.MidSegment;

public class CommitLogTest {
	@Test
	public void test() throws IOException {
		Configuration conf = new Configuration();
		conf.load("conf/index.conf");
		CommitLog.INSTANCE.init(conf);
		CommitLog.INSTANCE.openNewLog(0);
		for (int i = 0; i < 10; i++) {
			CommitLog.INSTANCE.write(Integer.toString(i), new MidSegment(i,
					new Segment(i, i, i, i)));
		}
		CommitLog.INSTANCE.shutdown();

		conf = new Configuration();
		conf.load("conf/index.conf");
		System.out.println(CommitLog.INSTANCE.dumpLog(0));
	}
}
