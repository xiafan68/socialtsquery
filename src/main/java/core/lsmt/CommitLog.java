package core.lsmt;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import Util.Configuration;
import Util.Pair;
import common.MidSegment;

/**
 * TODO: how to grantuee the thread safety of preVersions only one log file is
 * working at any time. * @author xiafan
 *
 */
public enum CommitLog {
	INSTANCE;

	private DataOutputStream dos = null;
	private File dir;
	private int curVersion = -1;
	private volatile int writeOpNum = 0;
	// log files that are not deleted
	private List<Integer> preVersions = new ArrayList<Integer>();

	// the set of words appearing in this log segment
	// ConcurrentSkipListSet<String> words = new
	// ConcurrentSkipListSet<String>();

	public void init(Configuration conf) {
		dir = conf.getCommitLogDir();
		if (!dir.exists()) {
			dir.mkdirs();
		}
	}

	private File versionFile(int version) {
		return new File(dir, String.format("%d.rlog", version));
	}

	/**
	 * flush the previous log for word, create a new version for word
	 * 
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
			curVersion = version;
		}
		try {
			dos = new DataOutputStream(new FileOutputStream(versionFile(version), true));
		} catch (FileNotFoundException e) {
			throw new RuntimeException(e);
		}
	}

	public void write(String word, MidSegment seg) {
		try {
			byte[] wordBytes = word.getBytes(Charset.forName("utf8"));
			dos.writeInt(wordBytes.length);
			dos.write(wordBytes);
			seg.write(dos);
			if (writeOpNum++ > 100) {
				writeOpNum = 0;
				dos.flush();
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	private final static Pattern RLOG_FILE_PATTERN = Pattern.compile("^[0-9]+.rlog$");

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
				int version1 = Integer.parseInt(o1.getName().substring(0, o1.getName().indexOf('.')));
				int version2 = Integer.parseInt(o2.getName().substring(0, o2.getName().indexOf('.')));
				return Integer.compare(version1, version2);
			}
		});
		for (File logFile : files) {
			redo(logFile, tree);
		}
	}

	private void redo(File file, LSMOInvertedIndex tree) throws IOException {
		DataInputStream dis = new DataInputStream(new FileInputStream(file));
		try {
			while (dis.available() > 0) {
				MidSegment seg = new MidSegment();
				int len = dis.readInt();
				byte[] bytes = new byte[len];
				dis.readFully(bytes);
				String word = new String(bytes, Charset.forName("utf8"));
				seg.read(dis);
				tree.insert(word, seg);
			}
		} catch (Exception ex) {

		}
		tree.maySwitchMemtable();
	}

	/**
	 * used for test
	 * 
	 * @param file
	 * @return
	 * @throws IOException
	 */
	public List<Pair<String, MidSegment>> dumpLog(int version) throws IOException {
		DataInputStream dis = new DataInputStream(new FileInputStream(versionFile(version)));
		List<Pair<String, MidSegment>> ret = new ArrayList<Pair<String, MidSegment>>();
		try {
			while (dis.available() > 0) {
				MidSegment seg = new MidSegment();
				int len = dis.readInt();
				byte[] bytes = new byte[len];
				dis.read(bytes);
				String word = new String(bytes, Charset.forName("utf8"));
				seg.readFields(dis);
				System.out.println(seg);
				ret.add(new Pair<String, MidSegment>(word, seg));
			}
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		return ret;
	}

	/**
	 * delete all logs with version not greater than version
	 * 
	 * @param version
	 */
	public void deleteLogs(int version) {
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
	 * 
	 * @param word
	 * @param version
	 */
	public void deleteLog(int version) {
		File logFile = versionFile(version);
		if (logFile.exists()) {
			logFile.delete();
		}
	}

	public void shutdown() throws IOException {
		dos.close();
	}
}
