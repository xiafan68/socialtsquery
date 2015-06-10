package core.index;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import common.MidSegment;

/**
 * used to record data, for each posting list, there is a operation log file for each in-memory octree.
 * @author xiafan
 *
 */
public class OperationLog {
	DataOutputStream dos = null;
	File dir;

	public OperationLog(String dir) {
		this.dir = new File(dir);
	}

	/**
	 * when the corresponding octree has been writen out.
	 * @param word
	 * @param version
	 */
	public void evictLog(int version) {
		File logFile = versionFile(version);
		if (logFile.exists()) {
			logFile.delete();
		}
	}

	private File versionFile(int version) {
		return new File(dir, String.format("%d.rlog", version));
	}

	/**
	 * flush the previous log for word, create a new version for word
	 * @param word
	 */
	public void openNewLog(int version) {
		if (dos != null) {
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

	public void write(MidSegment seg) {
		try {
			seg.write(dos);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	private final static Pattern RLOG_FILE_PATTERN = Pattern
			.compile("^[0-9]+.rlog$");

	public void recover(LogStructureOctree tree) throws IOException {
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

	private void redo(File file, LogStructureOctree tree) throws IOException {
		DataInputStream dis = new DataInputStream(new FileInputStream(file));
		while (dis.available() > 0) {
			MidSegment seg = new MidSegment();
			seg.read(dis);
			tree.insert(seg);
		}
		tree.flushMemOctree();
	}
}
