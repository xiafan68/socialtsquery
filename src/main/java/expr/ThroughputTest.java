package expr;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import expr.WorkLoadGen.WorkLoad;
import expr.runner.LocalTestRunner;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import util.Profile;

/**
 * 统计查询，更新在每个时间段的平均值
 * 
 * @author xiafan
 *
 */
public class ThroughputTest {
	Thread workLoadGenThread;
	Thread[] workers;
	AtomicBoolean stopped = new AtomicBoolean(false);
	String confFile;
	String seedFile;
	String updateFile;

	public ThroughputTest(String confFile, String seedFile, String updateFile) {
		super();
		this.confFile = confFile;
		this.seedFile = seedFile;
		this.updateFile = updateFile;
	}

	public void startTestThread() throws IOException {
		final WorkLoadGen gen = new WorkLoadGen(new float[] { 0.5f, 0.3f, 0.2f });
		gen.open(confFile, updateFile, seedFile);

		final BlockingQueue<WorkLoad> queue = gen.getJobQueue();

		workLoadGenThread = new Thread() {
			@Override
			public void run() {
				while (gen.hasNext()) {
					while (queue.size() < 10000 && gen.hasNext()) {
						gen.genNextJob();
					}
				}
				stopped.set(true);
			}
		};
		workLoadGenThread.start();

		// load runner
		final LocalTestRunner runner = new LocalTestRunner(confFile);
		runner.prepare();
		Thread[] workers = new Thread[3];
		for (int i = 0; i < workers.length; i++) {
			workers[i] = new Thread() {
				@Override
				public void run() {
					while (!queue.isEmpty() || !stopped.get()) {
						WorkLoad load;
						try {
							load = queue.poll(100, TimeUnit.MILLISECONDS);
							if (load != null)
								runner.execWorkLoad(load);
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
					}
				}
			};
			workers[i].start();
		}
		for (int i = 0; i < workers.length; i++) {
			try {
				workers[i].join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		runner.cleanup();
	}

	public static void main(String[] args) throws IOException {
		OptionParser parser = new OptionParser();
		parser.accepts("s", "query seed file").withRequiredArg().ofType(String.class);
		parser.accepts("d", "data file").withRequiredArg().ofType(String.class);
		parser.accepts("c", "index configuration file").withRequiredArg().ofType(String.class);
		parser.accepts("e", "profile result file").withRequiredArg().ofType(String.class);
		OptionSet opt = parser.parse(args);
		ThroughputTest test = new ThroughputTest((String) opt.valueOf("c"), (String) opt.valueOf("s"),
				(String) opt.valueOf("d"));
		Profile.instance.open((String) opt.valueOf("e"));
		test.startTestThread();
		Profile.instance.close();
	}
}
