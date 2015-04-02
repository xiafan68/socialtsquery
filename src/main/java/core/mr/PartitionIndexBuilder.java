package core.mr;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

/**
 * 
 * @author xiafan
 * @version 0.1 Mar 21, 2015
 */
public class PartitionIndexBuilder {
	/**
	 * 
	 * @param args
	 * @throws IOException
	 * @throws ClassNotFoundException
	 * @throws InterruptedException
	 */
	public static void main(String[] args) throws IOException,
			ClassNotFoundException, InterruptedException {
		if (args.length < 3) {
			System.out.println("usage: input dir, output dir, parallel level");
			return;
		}
		Path input = new Path(args[0]);
		final Path output = new Path(args[1]);

		Configuration conf = new Configuration();

		ExecutorService exec = Executors.newFixedThreadPool(Integer
				.parseInt(args[2]));
		FileSystem fs = FileSystem.get(input.toUri(), conf);
		FileStatus[] statuses = fs.listStatus(input, new PathFilter() {
			public boolean accept(Path arg0) {
				if (arg0.getName().contains("part"))
					return true;
				return false;
			}
		});
		for (FileStatus status : statuses) {
			final FileStatus curStatus = status;
			if (status.isDir()) {
				if (status.getPath().getName().length() != 5) {
					System.out.println("skip " + status.getPath().toString());
					continue;
				}
				exec.execute(new Runnable() {
					public void run() {
						for (int i = 0; i < 3; i++) {
							try {
								InvertedIndexBuilder.main(new String[] {
										curStatus.getPath().toString(),
										new Path(output, curStatus.getPath()
												.getName()).toString() });
								break;
							} catch (Exception e) {
								e.printStackTrace();
							}
						}// end of for
					}
				});
			}
		}
	}
}
