package example;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Scanner;

import joptsimple.OptionParser;
import joptsimple.OptionSet;

import org.apache.log4j.PropertyConfigurator;

import Util.Configuration;
import core.lsmo.OctreeBasedLSMTFactory;
import core.lsmt.LSMTInvertedIndex;
import segmentation.Interval;

public class IndexConsoleClient {

	public static void main(String[] args) throws IOException {
		OptionParser parser = new OptionParser();
		parser.accepts("c", "index configuration file").withRequiredArg()
				.ofType(String.class);
		parser.accepts("l", "log4j configuration file").withRequiredArg()
				.ofType(String.class);
		parser.accepts("d", "data file location").withRequiredArg()
				.ofType(String.class);
		OptionSet opts = null;
		try {
			opts = parser.parse(args);
		} catch (Exception exception) {
			parser.printHelpOn(System.out);
			return;
		}

		PropertyConfigurator.configure(opts.valueOf("l").toString());
		Configuration conf = new Configuration();
		conf.load(opts.valueOf("c").toString());
		LSMTInvertedIndex client = new LSMTInvertedIndex(conf,
				OctreeBasedLSMTFactory.INSTANCE);
		try {
			client.init();
		} catch (IOException e) {
			e.printStackTrace();
			client = null;
		}

		/*	try {
				System.setErr(new PrintStream(new FileOutputStream("/tmp/err.txt", false)));
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			}
		*/
		Scanner scanner = new Scanner(System.in);
		System.out.print("interval query>");
		String line = scanner.nextLine();
		try {
			while (!line.startsWith("quit")) {
				List<String> mids = new ArrayList<String>();
				Iterator<Interval> answer = null;

				args = line.split("[ ]+");
				long startTime = System.currentTimeMillis();
				try {
					if (args[0].equals("query")) {
						int k = Integer.parseInt(args[1]);
						int start = Integer.parseInt(args[2]);
						int end = Integer.parseInt(args[3]);
						List<String> keywords = new ArrayList<String>();
						for (int i = 4; i < args.length; i++) {
							keywords.add(args[i]);
						}
						answer = client.query(keywords, start, end, k);
					} else if (args[0].equals("queryall")) {
						String keywords = "";
						for (int i = 1; i < args.length; i++) {
							keywords += " " + args[i];
						}
						// answer = index.queryWithScore(keywords.trim());
					} else if (args[0].equals("debug")) {

					}
				} catch (Exception ex) {
					ex.printStackTrace();
					client.close();
				}
				long cost = System.currentTimeMillis() - startTime;

				if (answer != null) {
					while (answer.hasNext()) {
						Interval inv = answer.next();
						System.out.println(inv);
						mids.add(Long.toString(inv.getMid()));
					}
				}

				System.out.println("size: " + mids.size());

				System.out.println("time cost:" + cost / 1000.0 + "s");
				System.out.print("interval query>");
				line = scanner.nextLine();
			}
		} catch (Exception ex) {
			ex.printStackTrace();
		} finally {
			client.close();
		}
		scanner.close();
	}
}
