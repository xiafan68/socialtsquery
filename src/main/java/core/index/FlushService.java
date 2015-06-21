package core.index;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentSkipListSet;

/**
 * responsible for write data to disk
 * @author xiafan
 *
 */
public class FlushService {
	LSMOInvertedIndex index;
	List<FlushWorker> workers = new ArrayList<FlushWorker>();
	ConcurrentSkipListSet<String> pendingWords = new ConcurrentSkipListSet<String>();
	ConcurrentSkipListSet<String> flushingWords = new ConcurrentSkipListSet<String>();

	public FlushService(LSMOInvertedIndex index) {
		this.index = index;
	}

	public void start() {
		for (int i = 0; i < index.getFlushNum(); i++) {
			FlushWorker worker = new FlushWorker();
			worker.start();
			workers.add(worker);
		}
	}

	public void stop() {
		while (!workers.isEmpty()) {
			FlushWorker worker = workers.get(0);
			try {
				worker.join();
				workers.remove(0);
			} catch (Exception e) {
			}
		}
	}

	private class FlushWorker extends Thread {
		public FlushWorker() {
			super("flush worker");
		}

		@Override
		public void run() {
			while (index.running.get()) {
				Iterator<String> iter = pendingWords.iterator();
				while (iter.hasNext()) {
					String word = iter.next();
					iter.remove();
					if (flushingWords.add(word)) {
						LogStructureOctree tree = index.getPostingList(word);
						if (tree != null) {
							flushingWords.remove(word);
						}
					}
				}
			}
		}

		/**
		 * an indication that posting list of word need to be flushed
		 * @param word
		 */
		public void register(String word) {
			pendingWords.add(word);
		}
	}
}
