package expr;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import util.Configuration;

/**
 * 用于生成查询负载的类,负载生成由三部分构成： 1. 实时更新流，同时按照指定间隔更新流行关键词 2. 实时查询流，根据流行关键词，发起查询 3.
 * 历史查询流，根据统计好的历史热门词进行查询
 * 
 * @author xiafan
 *
 */
public class WorkLoadGen {
	BlockingQueue<WorkLoad> jobQueue = new LinkedBlockingQueue<WorkLoad>();

	float[] fractions;
	Random toss = new Random();

	Configuration conf;
	final float ratio;

	public WorkLoadGen(float[] fractions) {
		this.fractions = fractions;
		ratio = fractions[1] / (fractions[0] + fractions[1]);
	}

	public void open(String confFile, String updateFile, String seedFile) {
		try {

			conf = new Configuration();
			conf.load(confFile);
		} catch (IOException e) {
			return;
		}
	}

	public WorkLoad genNextJob() {
		WorkLoad ret = null;
		return ret;
	}

	public void genNextJobIntoQueue() {
		jobQueue.offer(genNextJob());
	}

	private BlockingQueue<String> wordQueue = new LinkedBlockingQueue<String>();

	public BlockingQueue<WorkLoad> getJobQueue() {
		return jobQueue;
	}

	public static class WorkLoad {
		public WorkLoad(int jobType, Object data) {
			this.type = jobType;
			this.data = data;
		}

		public static final int INSERT_JOB = 0;
		public static final int QUEYR_JOB = 1;
		public int type;
		public Object data;

		@Override
		public String toString() {
			return "WorkLoad [type=" + type + ", data=" + data + "]";
		}
	}

}
