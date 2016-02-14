package expr;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
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
public class ParaReadWriteTest {
	Thread workLoadGenThread;
	Thread[] workers;
	AtomicBoolean stopped = new AtomicBoolean(false);
	BlockingQueue<WorkLoad> writeQueue;

	InsertStreamGen iGen;
	ReadStreamGen rGen;
	LocalTestRunner runner;

	Thread[] readWorkers;
	Thread[] writeWorkers;

	public ParaReadWriteTest() {
	}

	public void init(String confFile, String seedFile, String updateFile) throws IOException {
		// load runner
		runner = new LocalTestRunner(confFile);
		runner.prepare();
		iGen = new InsertStreamGen();
		iGen.init(confFile, updateFile);

		rGen = new ReadStreamGen(new float[] { 0.8f, 0.2f });
		rGen.init(seedFile, confFile);
		iGen.setSub(rGen);
	}

	private void startInsertJobs() {
		// write threads
		writeWorkers = new Thread[1];
		for (int i = 0; i < writeWorkers.length; i++) {
			writeWorkers[i] = new Thread() {
				@Override
				public void run() {
					WorkLoad load = null;
					while (iGen.hasNext()) {
						load = iGen.genNextJob();
						runner.execWorkLoad(load);
					}
					stopped.set(true);
				}
			};
			writeWorkers[i].start();
		}
	}

	private void startReadJobs() {
		// read workers
		readWorkers = new Thread[1];
		for (int i = 0; i < readWorkers.length; i++) {
			readWorkers[i] = new Thread() {
				@Override
				public void run() {
					WorkLoad load;
					while (!stopped.get()) {
						load = rGen.genNextJob();
						runner.execWorkLoad(load);
					}
				}
			};
			readWorkers[i].start();
		}
	}

	public void startTestThread() throws IOException {
		startInsertJobs();
		startReadJobs();

		for (int i = 0; i < writeWorkers.length; i++) {
			while (true) {
				try {
					writeWorkers[i].join();
					break;
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}

		for (int i = 0; i < readWorkers.length; i++) {
			while (true) {
				try {
					readWorkers[i].join();
					break;
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
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
		ParaReadWriteTest test = new ParaReadWriteTest();
		test.init((String) opt.valueOf("c"), (String) opt.valueOf("s"), (String) opt.valueOf("d"));
		Profile.instance.startPeriodReport((String) opt.valueOf("e"), 1000 * 60 * 10);
		test.startTestThread();
		Profile.instance.close();
	}
}
