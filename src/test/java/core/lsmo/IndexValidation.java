package core.lsmo;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.log4j.PropertyConfigurator;
import org.junit.Test;

import Util.Configuration;
import core.lsmt.LSMTInvertedIndex;
import segmentation.Interval;

public class IndexValidation {
	@Test
	public void crossValidate() throws IOException {
		PropertyConfigurator.configure("conf/log4j-server2.properties");
		Configuration conf = new Configuration();
		conf.load("conf/index_twitter.conf");
		LSMTInvertedIndex bdbIndex = new LSMTInvertedIndex(conf);
		try {
			bdbIndex.init();
		} catch (IOException e) {
		}

		conf = new Configuration();
		conf.load("conf/index_twitter_intern.conf");
		LSMTInvertedIndex internIndex = new LSMTInvertedIndex(conf);
		try {
			internIndex.init();
		} catch (IOException e) {
		}

		HashMap<Long, Interval> bdb = new HashMap<Long, Interval>();
		Iterator<Interval> iter = bdbIndex.query(Arrays.asList("time"), 0, 701184, 30, "weighted".toUpperCase());
		while (iter.hasNext()) {
			Interval inv = iter.next();
			bdb.put(inv.getMid(), inv);
		}

		HashMap<Long, Interval> intern = new HashMap<Long, Interval>();
		iter = internIndex.query(Arrays.asList("time"), 0, 701184, 30, "weighted".toUpperCase());
		while (iter.hasNext()) {
			Interval inv = iter.next();
			if (bdb.containsKey(inv.getMid())) {
				bdb.remove(inv.getMid());
			} else {
				intern.put(inv.getMid(), inv);
			}
		}

		for (Interval inv : bdb.values()) {
			System.out.println(inv);
		}
		System.out.println("_______________________________");
		// System.out.println(intern);
		for (Interval inv : intern.values()) {
			System.out.println(inv);
		}
		bdbIndex.close();
		internIndex.close();
	}

}
