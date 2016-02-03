package util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.collections.BidiMap;
import org.apache.commons.collections.bidimap.TreeBidiMap;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import common.MidSegment;
import core.lsmt.LSMTInvertedIndex;
import io.DirLineReader;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import segmentation.ISegmentation.ISegSubscriber;
import segmentation.Interval;
import segmentation.SWSegmentation;
import segmentation.Segment;
import shingle.ITextShingle;
import shingle.ShingleFactory;
import weibo.Tweet;

public class IndexLoader {
	private static Logger logger = Logger.getLogger(IndexLoader.class);

	BlockingQueue<Pair<List<String>, MidSegment>> queue = new ArrayBlockingQueue<Pair<List<String>, MidSegment>>(10000);
	String inputFile;
	String confFile;
	String log;
	LSMTInvertedIndex index;
	volatile boolean noMore = false;

	public IndexLoader(String conf, String log, String inputFile) {
		this.inputFile = inputFile;
		this.confFile = conf;
		this.log = log;
	}

	public void init() throws IOException {
		PropertyConfigurator.configure(log);
		conf = new Configuration();
		conf.load(confFile);

		try {
			// FileUtils.deleteDirectory(conf.getIndexDir());
			conf.getIndexDir().mkdirs();
			// FileUtils.deleteDirectory(conf.getCommitLogDir());
			conf.getCommitLogDir().mkdirs();
			System.out.println(conf.toString());
		} catch (Exception exception) {

		}
		index = new LSMTInvertedIndex(conf);
		try {
			index.init();
		} catch (IOException e) {
			e.printStackTrace();
			index = null;
		}
	}

	Configuration conf;
	Thread producer;
	AtomicInteger midGen = new AtomicInteger(-1);
	long largestNum = -2;

	private void startProducer() {
		producer = new Thread("producer") {
			int minTime = conf.queryStartTime();

			@Override
			public void run() {
				DirLineReader reader = null;
				try {
					reader = new DirLineReader(inputFile);
				} catch (IOException e) {
					return;
				}
				String line = null;
				int i = 0;
				long start = System.currentTimeMillis();
				while (null != (line = reader.readLine())) {
					// parseSeries(line);
					parseTweetSegs(line);
					if (++i % 1000 == 0) {
						long time = System.currentTimeMillis() - start;
						logger.info("inserting " + i + " items costs " + time + " ms, " + " average "
								+ ((double) time / i) + " ms/i");
					}
				}
				try {
					reader.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
				noMore = true;
			}

			ITextShingle shingle = ShingleFactory.createShingle();
			private BidiMap mapping = new TreeBidiMap();

			private void parseSegs(String line) {
				MidSegment seg = new MidSegment();
				seg.parse(line);
				while (true) {
					try {
						queue.put(new Pair<List<String>, MidSegment>(Arrays.asList(Long.toString(seg.getMid())), seg));
						break;
					} catch (InterruptedException e) {
					}
				}
			}

			private void parseTweetSegs(String line) {
				int idx = line.lastIndexOf('\t');
				if (idx < 0) {
					return;
				}
				String tweetField = line.substring(0, idx);
				String histField = line.substring(idx + 1);
				Segment seg = new Segment();
				seg.parse(histField);
				seg.setStart(seg.getStart() - minTime);
				seg.setEndTime(seg.getEndTime() - minTime);
				Tweet tweet = new Tweet();
				tweet.parse(tweetField);
				long mid = -1;
				try {
					mid = Long.parseLong(tweet.getMid());
				} catch (Exception ex) {
					// logger.error(ex.getMessage());
					/*
					 * if (mapping.containsKey(tweet.getMid())) { mid =
					 * mapping.get(tweet.getMid()); } else { mid = 0 -
					 * mapping.size(); mapping.put(tweet.getMid(), mid); }
					 */
					if (mapping.containsKey(tweet.getMid())) {
						mid = (long) mapping.get(tweet.getMid());
					} else {
						while (mapping.size() > 500000) {
							if (mapping.containsValue(largestNum)) {
								mapping.removeValue(largestNum);
							}
							largestNum--;
						}
						mid = midGen.addAndGet(-1);
						mapping.put(tweet.getMid(), mid);
					}
				}

				try {
					final List<String> words = shingle.shingling(tweet.getContent(), false);
					while (true) {
						try {
							queue.put(new Pair<List<String>, MidSegment>(words, new MidSegment(mid, seg)));
							break;
						} catch (InterruptedException e) {
						}
					}
				} catch (IOException e) {
					e.printStackTrace();
				}
			}

			private void parseSeries(String line) {
				int idx = line.lastIndexOf('\t');
				if (idx < 0) {
					return;
				}
				Histogram hist = new Histogram();

				String tweetField = line.substring(0, idx);
				String histField = line.substring(idx + 1);
				Tweet tweet = new Tweet();
				tweet.parse(tweetField);
				hist.fromString(histField);
				long midTmp = -1;
				try {
					midTmp = Long.parseLong(tweet.getMid());
				} catch (Exception ex) {
					UUID uid = UUID.randomUUID();
					midTmp = uid.getLeastSignificantBits() & uid.getMostSignificantBits();
				}
				final long mid = midTmp;
				final List<String> words = new ArrayList<String>();
				try {
					words.addAll(shingle.shingling(tweet.getContent(), false));
				} catch (IOException e1) {
					e1.printStackTrace();
				}
				SWSegmentation seg = new SWSegmentation(mid, 5, null, new ISegSubscriber() {
					@Override
					public void newSeg(Interval preInv, Segment seg) {
						while (true) {
							try {
								queue.put(new Pair<List<String>, MidSegment>(words, new MidSegment(mid, seg)));
								break;
							} catch (InterruptedException e) {
							}
						}
					}
				});
				Iterator<Entry<Double, Integer>> iter = hist.groupby(1000 * 60 * 30).iterator();
				while (iter.hasNext()) {
					Entry<Double, Integer> entry = iter.next();
					seg.advance(entry.getKey().intValue(), entry.getValue());
				}
				seg.finish();
			}
		};
		producer.start();
	}

	Thread[] consumersThreads = null;

	private void startConsumers() {
		int count = 1;
		consumersThreads = new Thread[count];
		for (int i = 0; i < count; i++) {
			consumersThreads[i] = new Thread("consumer") {

				@Override
				public void run() {
					Pair<List<String>, MidSegment> cur = null;
					while (!queue.isEmpty() || !noMore) {
						try {
							while (null != (cur = queue.poll(10, TimeUnit.MILLISECONDS)) || !noMore) {
								if (cur != null)
									index.insert(cur.getKey(), cur.getValue());
							}
						} catch (InterruptedException e) {
							e.printStackTrace();
						} catch (IOException e) {
							e.printStackTrace();
						}
					}
				}

			};
			consumersThreads[i].start();
		}
	}

	public void dump() throws IOException {
		startProducer();
		startConsumers();
		for (int i = 0; i < consumersThreads.length; i++) {
			while (true) {
				try {
					consumersThreads[i].join();
					break;
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
		index.closeOnWait();
	}

	public static void main(String[] args) throws IOException {
		OptionParser parser = new OptionParser();
		parser.accepts("c", "index configuration file").withRequiredArg().ofType(String.class);
		parser.accepts("l", "log4j configuration file").withRequiredArg().ofType(String.class);
		parser.accepts("d", "data file location").withRequiredArg().ofType(String.class);
		OptionSet opts = null;
		try {
			opts = parser.parse(args);
		} catch (Exception exception) {
			parser.printHelpOn(System.out);
			return;
		}

		IndexLoader loader = new IndexLoader(opts.valueOf("c").toString(), opts.valueOf("l").toString(),
				opts.valueOf("d").toString());
		loader.init();
		loader.dump();
	}
}
