package Util;

import java.io.IOException;

import org.apache.log4j.PropertyConfigurator;

import common.MidSegment;

import core.index.LSMOInvertedIndex;
import fanxia.file.DirLineReader;

public class IndexLoader {

	public static void main(String[] args) throws IOException {
		PropertyConfigurator.configure("conf/log4j-server.properties");
		Configuration conf = new Configuration();
		conf.load("conf/index.conf");

		LSMOInvertedIndex index = new LSMOInvertedIndex(conf);
		try {
			index.init();
		} catch (IOException e) {
			e.printStackTrace();
			index = null;
		}

		// "/Users/xiafan/Documents/dataset/expr/twitter/twitter_segs"
		DirLineReader reader = new DirLineReader(args[0]);
		String line = null;
		int i = 0;
		while (null != (line = reader.readLine())) {
			MidSegment seg = new MidSegment();
			seg.parse(line);
			index.insert(Long.toString(seg.getMid()), seg);
			if (i++ % 1000 == 0) {
				// System.out.println(i);
			}
		}
		index.close();
		reader.close();
	}
}
