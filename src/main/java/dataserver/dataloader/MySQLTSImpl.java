package dataserver.dataloader;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.Properties;

import xiafan.util.Pair;

/**
 * delimiter $$

CREATE TABLE `tseries` (
  `id` varchar(30) NOT NULL,
  `time` bigint(20) DEFAULT NULL,
  `freq` int(11) DEFAULT NULL,
  KEY `index1` (`id`,`time`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1$$


 * @author xiafan
 *
 */
public class MySQLTSImpl implements ITSeriesDB {
	private static final String TABLE = "tseries";
	Connection conn = null; // 定义一个MYSQL链接对象
	boolean isBatch = false;
	int batchCount = 0;
	HashMap<String, List<Pair<Timestamp, Integer>>> batch = new HashMap<String, List<Pair<Timestamp, Integer>>>();

	public MySQLTSImpl() {

	}

	@Override
	public boolean open(Properties prop) {
		try {
			Class.forName("com.mysql.jdbc.Driver").newInstance(); // MYSQL驱动
			conn = DriverManager.getConnection(prop.getProperty("Server"),
					prop.getProperty("User"), prop.getProperty("Passwd")); // 链接本地MYSQL
			if (prop.containsKey("Batch")) {
				isBatch = Boolean.parseBoolean(prop.getProperty("Batch"));
			}
		} catch (Exception ex) {
			ex.printStackTrace();
			conn = null;
			return false;
		}
		return true;
	}

	@Override
	public void addPoint(String id, Timestamp time, int freq) {
		if (isBatch) {
			List<Pair<Timestamp, Integer>> list = getOrPutList(id);
			list.add(new Pair<Timestamp, Integer>(time, freq));
			batchCount++;
			if (batchCount > 1000) {
				batchUpdate();
			}
		} else {

		}
	}

	private void batchUpdate() {
		if (batch.isEmpty())
			return;

		StringBuffer sqlBuf = new StringBuffer("insert into " + TABLE
				+ "(id, time, freq) values");
		for (Entry<String, List<Pair<Timestamp, Integer>>> entry : batch
				.entrySet()) {
			for (Pair<Timestamp, Integer> pair : entry.getValue()) {
				sqlBuf.append(String.format("(%s,%d,%d),", entry.getKey(),
						pair.arg0.time, pair.arg1));
			}
		}
		sqlBuf.setCharAt(sqlBuf.length() - 1, ';');
		Statement stmt;
		try {
			stmt = conn.createStatement();
			stmt.executeUpdate(sqlBuf.toString());
			batchCount = 0;
			batch.clear();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public List<Pair<Timestamp, Integer>> getOrPutList(String id) {
		if (!batch.containsKey(id)) {
			batch.put(id, new ArrayList<Pair<Timestamp, Integer>>());
		}
		return batch.get(id);
	}

	@Override
	public void addSeries(String id, List<Pair<Timestamp, Integer>> series) {
		for (Pair<Timestamp, Integer> pair : series) {
			addPoint(id, pair.arg0, pair.arg1);
		}
	}

	@Override
	public int sum(String id, Timestamp start, Timestamp end) {
		int ret = 0;
		String sql = "select sum(freq) from " + TABLE + " where id = " + id
				+ " and time >=" + start.time + " and time <=" + end.time;
		Statement stmt;
		try {
			stmt = conn.createStatement();
			ResultSet set = stmt.executeQuery(sql);
			ret = set.getInt(1);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return ret;
	}

	@Override
	public void close() {
		if (isBatch)
			batchUpdate();
		if (conn != null)
			try {
				conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}

	}

}
