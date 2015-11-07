package example;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.log4j.PropertyConfigurator;

import Util.Configuration;
import core.lsmi.SortedListBasedLSMTFactory;
import core.lsmt.LSMTInvertedIndex;
import jline.TerminalFactory;
import jline.console.ConsoleReader;
import jline.console.completer.ArgumentCompleter;
import jline.console.completer.CandidateListCompletionHandler;
import jline.console.completer.Completer;
import jline.console.completer.FileNameCompleter;
import jline.console.completer.NullCompleter;
import jline.console.completer.StringsCompleter;
import jline.console.history.MemoryHistory;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import segmentation.Interval;

public class IndexConsoleClient {

	private static ConsoleReader buildReader() throws IOException {
		// 设置jline相关设置
		ConsoleReader reader = new ConsoleReader();
		reader.setPrompt("tquery> ");
		String line;
		MemoryHistory history = new MemoryHistory();
		history.setMaxSize(20);
		reader.setHistory(history);
		CandidateListCompletionHandler handler = new CandidateListCompletionHandler();
		reader.setCompletionHandler(handler);

		TerminalFactory.configure(TerminalFactory.UNIX);

		List<Completer> comps = new ArrayList<Completer>();
		comps.add(new StringsCompleter("query", "stats", "quit"));
		comps.add(new FileNameCompleter());

		List<Completer> completors = new LinkedList<Completer>();
		completors.add(new ArgumentCompleter(new StringsCompleter("quit"), new NullCompleter()));
		completors.add(new ArgumentCompleter(new StringsCompleter("stats"), new NullCompleter()));
		completors.add(new ArgumentCompleter(new StringsCompleter("query"),
				new StringsCompleter("and", "or", "weighted"), new NullCompleter()));
		for (Completer c : completors) {
			reader.addCompleter(c);
		}

		return reader;
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

		PropertyConfigurator.configure(opts.valueOf("l").toString());
		Configuration conf = new Configuration();
		conf.load(opts.valueOf("c").toString());
		LSMTInvertedIndex client = new LSMTInvertedIndex(conf, SortedListBasedLSMTFactory.INSTANCE);
		try {
			client.init();
		} catch (IOException e) {
			e.printStackTrace();
			client = null;
		}
		ConsoleReader reader = buildReader();
		String line = reader.readLine();
		PrintWriter out = new PrintWriter(reader.getOutput());
		try {
			while (!line.startsWith("quit")) {
				List<String> mids = new ArrayList<String>();
				Iterator<Interval> answer = null;

				args = line.split("[ ]+");
				long startTime = System.currentTimeMillis();
				try {
					if (args[0].equals("query")) {
						int i = 1;
						String type = args[i++];
						int k = Integer.parseInt(args[i++]);
						int start = Integer.parseInt(args[i++]);
						int end = Integer.parseInt(args[i++]);
						List<String> keywords = new ArrayList<String>();
						for (int idx = i; idx < args.length; idx++) {
							keywords.add(args[idx]);
						}
						answer = client.query(keywords, start, end, k, type.toUpperCase());
					} else if (args[0].equals("stats")) {
						Map<Float, Integer> stats = client.collectStatistics(args[1]);
						System.out.println(stats);
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

				out.println("size: " + mids.size());
				out.println("time cost:" + cost / 1000.0 + "s");
				line = reader.readLine();
			}
		} catch (Exception ex) {
			ex.printStackTrace();
		} finally {
			client.close();
		}
	}
}
