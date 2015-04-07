package dataserver.dataloader;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Properties;

import xiafan.file.DirLineReader;
import xiafan.util.Histogram;
import xiafan.util.Progress;

import common.Tweet;

public class TweetLoader {
	ITSeriesDB db;

	public void open(Properties prop) {
		db = new MySQLTSImpl();
		db.open(prop);
	}

	public void load(String dataDir) throws IOException {
		DirLineReader reader = new DirLineReader(dataDir);
		String line = null;
		Progress pro = new Progress(new Progress.DefaultReporter(), 100000000,
				0.00001f);
		int count = 0;
		while (null != (line = reader.readLine())) {
			String[] fields = line.split("\t");
			Tweet t = new Tweet();
			t.parse(fields[0]);

			Histogram hist = new Histogram();
			hist.fromString(fields[1]);
			hist = hist.groupby(3600000);
			Iterator<Entry<Double, Integer>> iter = hist.iterator();
			while (iter.hasNext()) {
				Entry<Double, Integer> entry = iter.next();
				db.addPoint(t.getMid(),
						new Timestamp(entry.getKey().intValue()),
						entry.getValue());
				pro.step();
			}
		}
		reader.close();
		db.close();
	}

	public static void main(String[] args) throws IOException {
		String dataDir = args[0];
		Properties prop = new Properties();
		prop.put("invdir", "./inv");
		prop.put("Server", "jdbc:mysql://localhost:3306/tseries");
		prop.put("User", "root");
		prop.put("Batch", "true");
		prop.put("Passwd", "Hadoop123");
		prop.put("write", "");
		TweetLoader loader = new TweetLoader();
		loader.open(prop);
		loader.load(dataDir);
	}
}
