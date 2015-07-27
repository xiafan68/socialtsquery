package Util;

import java.io.IOException;

import common.MidSegment;

import core.index.LSMOInvertedIndex;
import fanxia.file.DirLineReader;

public class IndexLoader {

	public static void main(String[] args) throws IOException {
		Configuration conf = new Configuration();
		conf.load("conf/index.conf");
		LSMOInvertedIndex indexReader = new LSMOInvertedIndex(conf);
		try {
			indexReader.init();
		} catch (IOException e) {
			e.printStackTrace();
			indexReader = null;
		}
		DirLineReader reader = new DirLineReader(
				"/Users/xiafan/Documents/dataset/expr/twitter/twitter_segs");
		String line = null;
		int i = 0;
		while (null != (line = reader.readLine())) {
			MidSegment seg = new MidSegment();
			seg.parse(line);
			indexReader.insert(Long.toString(seg.getMid()), seg);
			if (i++ % 1000 == 0) {
				System.out.println(i);
			}
		}
		indexReader.close();
		reader.close();
	}
}
