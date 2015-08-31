package Util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import segmentation.ISegmentation.ISegSubscriber;
import segmentation.Interval;
import segmentation.SWSegmentation;
import segmentation.Segment;
import shingle.TextShingle;
import xiafan.util.Histogram;

import common.MidSegment;
import common.Tweet;

import core.lsmo.OctreeBasedLSMTFactory;
import core.lsmt.LSMTInvertedIndex;
import fanxia.file.DirLineReader;

public class IndexLoader {
	private static Logger logger = Logger.getLogger(IndexLoader.class);

	BlockingQueue<Pair<List<String>, MidSegment>> queue = new ArrayBlockingQueue<Pair<List<String>, MidSegment>>(
			10000);
	String inputFile;
	LSMTInvertedIndex index;
	volatile boolean noMore = false;

	public IndexLoader(String inputFile) {
		this.inputFile = inputFile;
	}

	public void init() throws IOException {
		PropertyConfigurator.configure("conf/log4j-server.properties");
		Configuration conf = new Configuration();
		conf.load("conf/index.conf");

		index = new LSMTInvertedIndex(conf, OctreeBasedLSMTFactory.INSTANCE);
		try {
			index.init();
		} catch (IOException e) {
			e.printStackTrace();
			index = null;
		}
	}

	Thread producer;

	private void startProducer() {
		producer = new Thread() {
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
					parseSeries(line);

					if (++i % 1000 == 0) {
						long time = System.currentTimeMillis() - start;
						logger.info("inserting " + i + " items costs " + time
								+ " ms, " + " average " + ((double) time / i)
								+ " ms/i");
					}
				}
				try {
					reader.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
				noMore = true;
			}

			TextShingle shingle = new TextShingle(null);
			private Map<String, Long> mapping;

			private void parseSegs(String line) {
				MidSegment seg = new MidSegment();
				seg.parse(line);
				while (true) {
					try {
						queue.put(new Pair<List<String>, MidSegment>(Arrays
								.asList(Long.toString(seg.getMid())), seg));
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
				Tweet tweet = new Tweet();
				tweet.parse(tweetField);
				long mid = -1;
				try {
					mid = Long.parseLong(tweet.getMid());
				} catch (Exception ex) {
					// logger.error(ex.getMessage());
					if (mapping.containsKey(tweet.getMid())) {
						mid = mapping.get(tweet.getMid());
					} else {
						mid = 0 - mapping.size();
						mapping.put(tweet.getMid(), mid);
					}
				}

				try {
					final List<String> words = shingle.shingling(tweet
							.getContent());
					while (true) {
						try {
							queue.put(new Pair<List<String>, MidSegment>(words,
									new MidSegment(mid, seg)));
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
					midTmp = uid.getLeastSignificantBits()
							& uid.getMostSignificantBits();
				}
				final long mid = midTmp;
				final List<String> words = new ArrayList<String>();
				try {
					words.addAll(shingle.shingling(tweet.getContent()));
				} catch (IOException e1) {
					e1.printStackTrace();
				}
				SWSegmentation seg = new SWSegmentation(mid, 5, null,
						new ISegSubscriber() {
							@Override
							public void newSeg(Interval preInv, Segment seg) {
								while (true) {
									try {
										queue.put(new Pair<List<String>, MidSegment>(
												words, new MidSegment(mid, seg)));
										break;
									} catch (InterruptedException e) {
									}
								}
							}
						});
				Iterator<Entry<Double, Integer>> iter = hist.groupby(
						1000 * 60 * 30).iterator();
				while (iter.hasNext()) {
					Entry<Double, Integer> entry = iter.next();
					seg.advance(entry.getKey().intValue(), entry.getValue());
				}
				seg.finish();
			}
		};
		producer.start();
	}

	Thread[] consumersThreads = new Thread[10];

	private void startConsumers() {
		for (int i = 0; i < 10; i++) {
			consumersThreads[i] = new Thread() {

				@Override
				public void run() {
					Pair<List<String>, MidSegment> cur = null;
					while (!queue.isEmpty() || !noMore) {
						try {
							while (null != (cur = queue.poll(10,
									TimeUnit.MILLISECONDS)) || !noMore) {
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
		index.close();
	}

	public static void main(String[] args) throws IOException {
		IndexLoader loader = new IndexLoader(args[0]);
		loader.init();
		loader.dump();
	}
}
