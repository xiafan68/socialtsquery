package core.mr;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

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
		Path input = new Path(args[0]);
		Path output = new Path(args[1]);

		Configuration conf = new Configuration();

		FileSystem fs = FileSystem.get(input.toUri(), conf);
		FileStatus[] statuses = fs.listStatus(input);
		for (FileStatus status : statuses) {
			if (status.isDir()) {
				InvertedIndexBuilder
						.main(new String[] {
								status.getPath().toString(),
								new Path(output, status.getPath().getName())
										.toString() });
			}
		}
	}
}
