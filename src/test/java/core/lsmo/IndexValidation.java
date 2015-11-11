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

	public static LSMTInvertedIndex openIndex(String logPath, String confPath) throws IOException {
		PropertyConfigurator.configure(logPath);
		Configuration conf = new Configuration();
		conf.load(confPath);
		LSMTInvertedIndex index = new LSMTInvertedIndex(conf);
		try {
			index.init();
		} catch (IOException e) {
		}
		return index;
	}

	private static void validate(LSMTInvertedIndex indexA, LSMTInvertedIndex indexB) throws IOException {
		int ts = 0;
		int te = 700000;
		int topk = 300;
		HashMap<Long, Interval> bdb = new HashMap<Long, Interval>();
		Iterator<Interval> iter = indexA.query(Arrays.asList("time"), ts, te, topk, "weighted".toUpperCase());
		while (iter.hasNext()) {
			Interval inv = iter.next();
			bdb.put(inv.getMid(), inv);
		}
		System.out.println("size of bdb:" + bdb.size());

		HashMap<Long, Interval> intern = new HashMap<Long, Interval>();
		iter = indexB.query(Arrays.asList("time"), ts, te, topk, "weighted".toUpperCase());
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

		System.out.println(bdb.size() + "," + intern.size());
	}

	@Test
	public void crossValidateIntern() throws IOException {
		LSMTInvertedIndex bdbIndex = openIndex("conf/log4j-server2.properties", "conf/index_twitter.conf");
		LSMTInvertedIndex internIndex = openIndex("conf/log4j-server2.properties", "conf/index_twitter_intern.conf");
		validate(bdbIndex, internIndex);
		bdbIndex.close();
		internIndex.close();
	}

	@Test
	public void crossValidateLSMI() throws IOException {
		LSMTInvertedIndex bdbIndex = openIndex("conf/log4j-server2.properties", "conf/index_twitter.conf");
		LSMTInvertedIndex internIndex = openIndex("conf/log4j-server2.properties", "conf/index_twitter_lsmi.conf");
		validate(bdbIndex, internIndex);
		bdbIndex.close();
		internIndex.close();
	}

}
