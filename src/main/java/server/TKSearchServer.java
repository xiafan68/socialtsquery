package server;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TTransportException;

import common.MidSegment;
import core.commom.TempKeywordQuery;
import core.executor.WeightedQueryExecutor;
import core.lsmt.LSMTInvertedIndex;
import searchapi.FetchTweetQuery;
import searchapi.InvalidJob;
import searchapi.TKeywordQuery;
import searchapi.TweetSeg;
import searchapi.TweetService;
import searchapi.Tweets;
import segmentation.Interval;
import segmentation.Segment;
import tweetstore.CassandraConn;
import tweetstore.TweetDao;
import util.Configuration;
import util.Profile;

public class TKSearchServer implements TweetService.Iface {
	private static final Logger logger = Logger.getLogger(TKSearchServer.class);
	LSMTInvertedIndex indexReader;
	// JDBC jdbc;
	CassandraConn conn = new CassandraConn();
	TweetDao tweetDao = new TweetDao(conn);

	public void start() throws IOException {
		// Path dir = new Path("/Users/xiafan/temp/output/");
		// dir = new Path("/home/xiafan/temp/invindex_parts");
		conn.connect("127.0.0.1");

		Configuration conf = new Configuration();
		conf.load("conf/index_twitter_intern.conf");
		indexReader = new LSMTInvertedIndex(conf);
		try {
			indexReader.init();
		} catch (IOException e) {
			e.printStackTrace();
			indexReader = null;
		}
		// try {
		// jdbc = new
		// JDBC("jdbc:mysql://localhost:3306/tseries?useUnicode=true&characterEncoding=utf8",
		// "root",
		// "Hadoop123");
		// } catch (Exception ex) {
		// System.out.println(ex.getMessage());
		// }
	}

	public void stop() throws IOException, SQLException {
		if (indexReader != null) {
			indexReader.close();
			indexReader = null;
		}
		// jdbc.close();
	}

	@Override
	public List<Long> search(TKeywordQuery query) throws TException {
		List<Long> ret = new ArrayList<Long>();
		WeightedQueryExecutor executor = new WeightedQueryExecutor(indexReader);
		executor.setMaxLifeTime((int) Math.pow(2, 31));
		Interval window = new Interval(1, query.startTime, query.endTime, 1);
		String[] keywords = new String[query.keywords.size()];
		query.keywords.toArray(keywords);
		TempKeywordQuery tQuery = new TempKeywordQuery(keywords, window, query.topk);
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

	/*
	 * private void readContent(Map<Long, TweetTuple> tweetMap, List<Long> tids)
	 * { Map<Long, String> contents; try { contents = dao.readTweets(tids); for
	 * (Entry<Long, String> entry : contents.entrySet()) { TweetTuple curTuple =
	 * null; if (tweetMap.containsKey(entry.getKey())) { curTuple =
	 * tweetMap.get(entry.getKey()); } else { curTuple = new TweetTuple();
	 * tweetMap.put(entry.getKey(), curTuple); }
	 * curTuple.setContent(entry.getValue()); } } catch (SQLException e) {
	 * e.printStackTrace(); } }
	 * 
	 * public void readTimeSeries(Map<Long, TweetTuple> tweetMap, List<Long>
	 * tids) throws SQLException { TimeSeriesDao seriesDao = new
	 * TimeSeriesDao(jdbc); Map<Long, List<List<Integer>>> tsData =
	 * seriesDao.getTimeSeries(tids); for (Entry<Long, List<List<Integer>>>
	 * entry : tsData.entrySet()) { TweetTuple curTuple = null; if
	 * (tweetMap.containsKey(entry.getKey())) { curTuple =
	 * tweetMap.get(entry.getKey()); } else { curTuple = new TweetTuple();
	 * tweetMap.put(entry.getKey(), curTuple); }
	 * curTuple.setPoints(entry.getValue()); } }
	 */

	@Override
	public Tweets fetchTweets(FetchTweetQuery query) throws InvalidJob, TException {
		Tweets tweets = new Tweets();
		/*
		 * Map<Long, TweetTuple> tweetMap = new HashMap<Long, TweetTuple>(); try
		 * { readContent(tweetMap, query.getTids()); readTimeSeries(tweetMap,
		 * query.getTids()); } catch (Exception e) { e.printStackTrace(); }
		 * tweets.setTweetMap(tweetMap);
		 */

		return tweets;
	}

	public static void main(String[] args) throws TTransportException, IOException, SQLException {
		TServerTransport serverTransport = new TServerSocket(10000);
		TKSearchServer tserver = new TKSearchServer();
		tserver.start();
		TweetService.Processor processor = new TweetService.Processor(tserver);
		TServer masterTServer = new TThreadPoolServer(new TThreadPoolServer.Args(serverTransport).processor(processor));
		logger.info("Starting the simple server...");
		masterTServer.serve();
		logger.info("simple server stoped");
		tserver.stop();
	}

	@Override
	public void indexTweetSeg(TweetSeg seg) throws TException {
		logger.info("indexing " + seg.toString());
		try {
			String text = tweetDao.getStatusByMid(seg.mid);
			if (text == null) {
				throw new TException("content of " + seg.mid + " is not found!!!!");
			}

			indexReader.insert(text, new MidSegment(Long.parseLong(seg.mid),
					new Segment(seg.starttime, seg.startcount, seg.endtime, seg.endcount)));
		} catch (NumberFormatException | IOException e) {
			logger.error(e.getMessage());
			throw new TException(e.getMessage());
		}
	}

}
