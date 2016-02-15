package expr.runner;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;

import common.MidSegment;
import core.lsmt.LSMTInvertedIndex;
import expr.WorkLoadGen.WorkLoad;
import searchapi.TKeywordsQuery;
import util.Configuration;
import util.Pair;
import util.Profile;

public class LocalTestRunner implements ITestRunner {
	private final Logger logger = Logger.getLogger(LocalTestRunner.class);
	Configuration conf;
	LSMTInvertedIndex index;
	String confFile;
	AtomicInteger updateCount = new AtomicInteger(0);

	public LocalTestRunner(String confFile) {
		this.confFile = confFile;
	}

	public void prepare() {
		try {
			conf = new Configuration();
			conf.load(confFile);
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

	public void cleanup() throws IOException {
		index.close();
	}

	public void execWorkLoad(WorkLoad load) {
		logger.info(load);
		if (load.type == WorkLoad.INSERT_JOB) {
			execUpdate((Pair<List<String>, MidSegment>) load.data);
		} else {
			execQuery((TKeywordsQuery) load.data);
		}
	}

	@Override
	public void execQuery(TKeywordsQuery query) {
		Profile.instance.start("query");
		try {
			index.query(query.query, query.startTime, query.endTime, query.topk, query.type.toString());
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			Profile.instance.end("query");
		}
	}

	@Override
	public void execUpdate(Pair<List<String>, MidSegment> update) {
		Profile.instance.start("insert");
		updateCount.incrementAndGet();
		// if (updateCount.get() % 100000 == 0) {
		// Profile.instance.flushAndReset();
		// }

		try {
			index.insert(update.getKey(), update.getValue());
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			Profile.instance.end("insert");
		}
	}

}
