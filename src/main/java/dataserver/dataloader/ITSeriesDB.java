package dataserver.dataloader;

import java.util.List;
import java.util.Properties;

import xiafan.util.Pair;

public interface ITSeriesDB {
	public boolean open(Properties prop);

	public void close();

	public void addPoint(String id, Timestamp time, int freq);

	public void addSeries(String id, List<Pair<Timestamp, Integer>> series);

	public int sum(String id, Timestamp start, Timestamp end);
}
