package server;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.apache.thrift.TException;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TTransportException;

import com.codahale.metrics.Timer.Context;

import casdb.CassandraConn;
import casdb.TweetDao;
import common.MidSegment;
import core.lsmt.LSMTInvertedIndex;
import dase.perf.MetricBasedPerfProfile;
import dase.perf.ServerController;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import searchapi.FetchTweetQuery;
import searchapi.InvalidJob;
import searchapi.TKeywordQuery;
import searchapi.TweetSeg;
import searchapi.TweetService;
import searchapi.Tweets;
import segmentation.Interval;
import segmentation.Segment;
import util.Configuration;
import weibo4j.org.json.JSONException;
import weibo4j.org.json.JSONObject;

public class TKSearchServer implements TweetService.Iface, ServerController.IServerSubscriber {
	private static final Logger logger = Logger.getLogger(TKSearchServer.class);
	TServer masterTServer;
	LSMTInvertedIndex indexReader;
	// JDBC jdbc;
	CassandraConn conn = new CassandraConn();
	TweetDao tweetDao;
	// 用于暂停或者停止服务器时使用的锁
	ReadWriteLock susLock = new ReentrantReadWriteLock();
	ServerController controler = new ServerController(this);
	AtomicInteger runningOps = new AtomicInteger(0);

	private void init(String casDB, String indexConf) throws IOException {
		conn.connect(casDB);
		tweetDao = new TweetDao(conn);
		Configuration conf = new Configuration();
		conf.load(indexConf);
		indexReader = new LSMTInvertedIndex(conf);
		try {
			try {
				conf.getIndexDir().mkdirs();
				conf.getCommitLogDir().mkdirs();
				System.out.println(conf.toString());
			} catch (Exception exception) {

			}
			indexReader.init();
		} catch (IOException e) {
			e.printStackTrace();
			indexReader = null;
		}
		logger.info("index initialized");
	}

	private void startThriftServer(Properties props) throws NumberFormatException, TTransportException {
		TServerTransport serverTransport = new TServerSocket(Short.parseShort(props.getProperty("thrift_port")));
		TweetService.Processor<TweetService.Iface> processor = new TweetService.Processor<TweetService.Iface>(this);
		masterTServer = new TThreadPoolServer(
				new TThreadPoolServer.Args(serverTransport).processor(processor).maxWorkerThreads(40000));
		logger.info("Starting the simple server...");
		controler.running();
		masterTServer.serve();
	}

	public void start(String confFile, String indexConf) throws Exception {
		MetricBasedPerfProfile.registerServer(controler);
		Properties props = new Properties();
		props.load(new FileInputStream(confFile));
		init(props.getProperty("cassdb"), indexConf);
		// now start the thrift server
		startThriftServer(props);
	}

	@Override
	public List<Long> search(TKeywordQuery query) throws TException {
		boolean locked = susLock.readLock().tryLock();
		try {
			if (locked) {
				runningOps.incrementAndGet();
				List<Long> ret = new ArrayList<Long>();
				Context timer = MetricBasedPerfProfile.timer("tksearch").time();
				try {
					Iterator<Interval> res = indexReader.query(query.query, query.startTime, query.endTime, query.topk,
							query.type.toString());
					while (res.hasNext())
						ret.add(res.next().getMid());
				} catch (IOException e) {
					e.printStackTrace();
				} finally {
					timer.stop();
				}
				return ret;
			} else {
				throw new TException("server is not ready to serve: ");
			}
		} finally {
			if (locked) {
				susLock.readLock().unlock();
				runningOps.decrementAndGet();
			}
		}
	}

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

	@Override
	public void indexTweetSeg(TweetSeg seg) throws TException {
		boolean locked = susLock.readLock().tryLock();
		try {
			if (locked) {
				runningOps.incrementAndGet();
				try {
					logger.info(runningOps.get() + " indexing " + seg.toString());
					Context dbTimer = MetricBasedPerfProfile.timer("tksearch_querycontent").time();
					String status = tweetDao.getStatusByMid(seg.mid);
					dbTimer.stop();
					if (status == null) {
						throw new TException("content of " + seg.mid + " is not found!!!!");
					} else {
						dbTimer = MetricBasedPerfProfile.timer("tksearch_index").time();
						try {
							JSONObject obj = new JSONObject(status);
							indexReader.insert(obj.getString("text"), obj.getString("uname"),
									new MidSegment(Long.parseLong(seg.mid),
											new Segment(seg.starttime, seg.startcount, seg.endtime, seg.endcount)));
						} finally {
							dbTimer.stop();
						}
					}
				} catch (NumberFormatException | IOException | JSONException e) {
					logger.error(e.getMessage());
					throw new TException(e.getMessage());
				}
			} else {
				throw new TException("server is not ready to serve: ");
			}
		} finally {
			if (locked) {
				susLock.readLock().unlock();
				runningOps.decrementAndGet();
			}
		}
	}

	@Override
	public void finalize() {
		close();
	}

	public void close() {
		logger.info("closing index");
		if (indexReader != null) {
			try {
				indexReader.close();
				indexReader = null;
			} catch (IOException e) {
				logger.error(e.getMessage());
				throw new RuntimeException(e);
			}
		}
		logger.info("stoping simple server");
		masterTServer.stop();
		logger.info("simple server stoped");
	}

	@Override
	public void onStop() {
		susLock.writeLock().lock();
		close();
	}

	@Override
	public void onSuspend() {
		susLock.writeLock().lock();
	}

	@Override
	public void onResume() {
		susLock.writeLock().unlock();
	}

	public static void main(String[] args) throws Exception {
		OptionParser parser = new OptionParser();
		parser.accepts("c", "configuration file of server").withRequiredArg().ofType(String.class);
		parser.accepts("i", "configuration file of index").withRequiredArg().ofType(String.class);
		parser.printHelpOn(new DataOutputStream(new PrintStream(System.out)));
		OptionSet set = parser.parse(args);

		PropertyConfigurator
				.configure(new File(System.getProperty("basedir", "./"), "conf/log4j.properties").getAbsolutePath());
		Properties props = new Properties();
		props.load(new FileInputStream(set.valueOf("c").toString()));

		MetricBasedPerfProfile.reportForGanglia(props.getProperty("gangliaHost"),
				Short.parseShort(props.getProperty("gangliaPort")));

		TKSearchServer tserver = new TKSearchServer();
		tserver.start(set.valueOf("c").toString(), set.valueOf("i").toString());
	}
}
