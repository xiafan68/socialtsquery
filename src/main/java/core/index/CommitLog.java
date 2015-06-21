package core.index;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import common.MidSegment;

/**
 *TODO: how to grantuee the thread safety of preVersions
 * only one log file is working at any time.
 *  * @author xiafan
 *
 */
public class CommitLog {
	DataOutputStream dos = null;
	File dir;
	int curVersion = -1;

	// log files that are not deleted
	List<Integer> preVersions = new ArrayList<Integer>();

	// the set of words appearing in this log segment
	ConcurrentSkipListSet<String> words = new ConcurrentSkipListSet<String>();

	public CommitLog(String dir) {
		this.dir = new File(dir);
	}

	private File versionFile(int version) {
		return new File(dir, String.format("%d.rlog", version));
	}

	/**
	 * flush the previous log for word, create a new version for word
	 * @not_thread_safe
	 * @param word
	 */
	public void openNewLog(int version) {
		if (dos != null) {
			if (curVersion != -1) {
				preVersions.add(curVersion);
			}
			try {
				dos.close();
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}
		try {
			dos = new DataOutputStream(new FileOutputStream(
					versionFile(version)));
		} catch (FileNotFoundException e) {
			throw new RuntimeException(e);
		}
	}

	public void write(String word, MidSegment seg) {
		words.add(word);
		try {
			dos.writeUTF(word);
			seg.write(dos);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	private final static Pattern RLOG_FILE_PATTERN = Pattern
			.compile("^[0-9]+.rlog$");

	public void recover(LSMOInvertedIndex tree) throws IOException {
		File[] files = dir.listFiles(new FilenameFilter() {
			@Override
			public boolean accept(File dir, String name) {
				Matcher matcher = RLOG_FILE_PATTERN.matcher(name);
				return matcher.matches();
			}
		});
		Arrays.sort(files, new Comparator<File>() {
			@Override
			public int compare(File o1, File o2) {
				int version1 = Integer.parseInt(o1.getName().substring(0,
						o1.getName().indexOf('.')));
				int version2 = Integer.parseInt(o2.getName().substring(0,
						o2.getName().indexOf('.')));
				return Integer.compare(version1, version2);
			}
		});
		for (File logFile : files) {
			redo(logFile, tree);
		}
	}

	private void redo(File file, LSMOInvertedIndex tree) throws IOException {
		DataInputStream dis = new DataInputStream(new FileInputStream(file));
		while (dis.available() > 0) {
			MidSegment seg = new MidSegment();
			String word = dis.readUTF();
			seg.read(dis);
			tree.insert(word, seg);
		}
		tree.flushMemtable();
	}

	/**
	 * delete all logs with version not greater than version
	 * @param version
	 */
	public void deleteLogs(int version) {
		// TODO Auto-generated method stub
		Iterator<Integer> iter = preVersions.iterator();
		while (iter.hasNext()) {
			int cur = iter.next();
			if (cur <= version) {
				deleteLog(cur);
			}
		}
	}

	/**
	 * when the corresponding octree has been writen out.
	 * @param word
	 * @param version
	 */
	public void deleteLog(int version) {
		File logFile = versionFile(version);
		if (logFile.exists()) {
			logFile.delete();
		}
	}
}
