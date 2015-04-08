package dataserver;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

public class TimeSeriesDao {
	JDBC jdbc;

	public TimeSeriesDao(JDBC jdbc) {
		this.jdbc = jdbc;
	}

	public Map<Long, List<List<Integer>>> getTimeSeries(List<Long> mids)
			throws SQLException {
		Map<Long, List<List<Integer>>> ret = new HashMap<Long, List<List<Integer>>>();
		String inCond = StringUtils.join(mids, ",");
		Connection con = jdbc.getCon();
		con.setCatalog("tseries");
		Statement stmt = con.createStatement();
		ResultSet set = stmt
				.executeQuery("select id, time, freq from tseries where id in ("
						+ inCond + ") order by id, time;");
		while (set.next()) {
			long mid = set.getLong("id");
			List<Integer> tuple = new ArrayList<Integer>();
			tuple.add(set.getInt("time"));
			tuple.add(set.getInt("freq"));
			List<List<Integer>> cur = null;
			if (ret.containsKey(mid)) {
				cur = ret.get(mid);
			} else {
				cur = new ArrayList<List<Integer>>();
				ret.put(mid, cur);
			}
			cur.add(tuple);
		}
		return ret;
	}
}
