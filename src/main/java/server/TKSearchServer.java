package server;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TTransportException;
//李超
//13661912248

import searchapi.FetchTweetQuery;
import searchapi.InvalidJob;
import searchapi.TKeywordQuery;
import searchapi.TweetService;
import searchapi.TweetTuple;
import searchapi.Tweets;
import segmentation.Interval;
import Util.Profile;
import core.executor.MultiPartitionExecutor;
import core.executor.PartitionExecutor;
import core.index.IndexReader;
import core.index.PartitionMeta;
import core.index.TempKeywordQuery;
import dataserver.JDBC;
import dataserver.TimeSeriesDao;
import dataserver.TweetDao;

public class TKSearchServer implements TweetService.Iface {
	private static final Logger logger = Logger.getLogger(TKSearchServer.class);
	IndexReader indexReader;
	JDBC jdbc;

	public void start() {
		Path dir = new Path("/Users/xiafan/temp/output/");
		dir = new Path("/home/xiafan/temp/invindex_parts");
		Configuration conf = new Configuration();

		indexReader = new IndexReader();
		try {
			indexReader.addPartitions(dir, conf);
			;
		} catch (IOException e) {
			e.printStackTrace();
			indexReader = null;
		}
		try {
			jdbc = new JDBC(
					"jdbc:mysql://localhost:3306/tseries?useUnicode=true&characterEncoding=utf8",
					"root", "Hadoop123");
		} catch (Exception ex) {
			System.out.println(ex.getMessage());
		}
	}

	public void stop() throws IOException, SQLException {
		if (indexReader != null) {
			indexReader.close();
			indexReader = null;
		}
		jdbc.close();
	}

	@Override
	public List<Long> search(TKeywordQuery query) throws TException {
		List<Long> ret = new ArrayList<Long>();
		MultiPartitionExecutor executor = new MultiPartitionExecutor(
				indexReader);
		executor.setMaxLifeTime((int) Math.pow(2, 31));
		Interval window = new Interval(1, query.startTime, query.endTime, 1);
		String[] keywords = new String[query.keywords.size()];
		query.keywords.toArray(keywords);
		TempKeywordQuery tQuery = new TempKeywordQuery(keywords, window,
				query.topk);
		Profile.instance.start("query");
		try {
			executor.query(tQuery);
			Iterator<Interval> res = executor.getAnswer();
			Profile.instance.end("query");
			while (res.hasNext())
				ret.add(res.next().getMid());
		} catch (IOException e) {
			e.printStackTrace();
		}

		System.out.println(Profile.instance.toJSON());
		return ret;
	}

	private void readContent(Map<Long, TweetTuple> tweetMap, List<Long> tids) {
		TweetDao dao = new TweetDao(jdbc);
		Map<Long, String> contents;
		try {
			contents = dao.readTweets(tids);
			for (Entry<Long, String> entry : contents.entrySet()) {
				TweetTuple curTuple = null;
				if (tweetMap.containsKey(entry.getKey())) {
					curTuple = tweetMap.get(entry.getKey());
				} else {
					curTuple = new TweetTuple();
					tweetMap.put(entry.getKey(), curTuple);
				}
				curTuple.setContent(entry.getValue());
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public void readTimeSeries(Map<Long, TweetTuple> tweetMap,
			FetchTweetQuery query) throws SQLException {
		TimeSeriesDao seriesDao = new TimeSeriesDao(jdbc);
		Map<Long, List<List<Integer>>> tsData = seriesDao.getTimeSeries(
				query.tids, query.startTime / 2, query.endTime / 2);
		for (Entry<Long, List<List<Integer>>> entry : tsData.entrySet()) {
			TweetTuple curTuple = null;
			if (tweetMap.containsKey(entry.getKey())) {
				curTuple = tweetMap.get(entry.getKey());
			} else {
				curTuple = new TweetTuple();
				tweetMap.put(entry.getKey(), curTuple);
			}
			curTuple.setPoints(entry.getValue());
		}
	}

	@Override
	public Tweets fetchTweets(FetchTweetQuery query) throws InvalidJob,
			TException {
		Tweets tweets = new Tweets();
		Map<Long, TweetTuple> tweetMap = new HashMap<Long, TweetTuple>();
		try {
			readContent(tweetMap, query.getTids());
			readTimeSeries(tweetMap, query);
		} catch (Exception e) {
			e.printStackTrace();
		}
		tweets.setTweetMap(tweetMap);

		return tweets;
	}

	public static void main(String[] args) throws TTransportException,
			IOException, SQLException {
		TServerTransport serverTransport = new TServerSocket(10000);

		TKSearchServer tserver = new TKSearchServer();
		tserver.start();
		TweetService.Processor processor = new TweetService.Processor(tserver);
		// Use this for a multithreaded server
		TServer masterTServer = new TThreadPoolServer(
				new TThreadPoolServer.Args(serverTransport)
						.processor(processor));

		logger.info("Starting the simple server...");
		masterTServer.serve();
		logger.info("simple server stoped");
		tserver.stop();
	}

}
