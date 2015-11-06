package core;

import java.io.IOException;

import jline.TerminalFactory;
import jline.console.ConsoleReader;
import jline.console.completer.CandidateListCompletionHandler;
import jline.console.completer.FileNameCompleter;
import jline.console.completer.StringsCompleter;
import jline.console.history.MemoryHistory;

public class ConsoleInterface {
	public static void main(String[] args) throws IOException {
		ConsoleReader reader = new ConsoleReader();
		reader.setPrompt("p> ");
		String line;
		MemoryHistory history = new MemoryHistory();
		history.setMaxSize(20);
		reader.setHistory(history);
		TerminalFactory.configure(TerminalFactory.UNIX);
		while ((line = reader.readLine()) != null) {
			if (line.equals("quit"))
				break;

		}
	}

	private void addCompletors(ConsoleReader reader) {
		reader.addCompleter(new FileNameCompleter());
		reader.addCompleter(new StringsCompleter("\u001B[1mfoo\u001B[0m", "bar", "\u001B[32mbaz\u001B[0m"));
		CandidateListCompletionHandler handler = new CandidateListCompletionHandler();
		reader.setCompletionHandler(handler);
	}
}
