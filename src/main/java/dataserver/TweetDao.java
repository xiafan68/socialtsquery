package dataserver;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

public class TweetDao {
	JDBC jdbc;

	public TweetDao(JDBC jdbc) {
		this.jdbc = jdbc;
	}

	public Map<Long, String> readTweets(List<Long> mids) throws SQLException {
		Map<Long, String> ret = new HashMap<Long, String>();
		String inCond = StringUtils.join(mids, ",");
		Connection con = jdbc.getCon();
		Statement stmt = con.createStatement();
		ResultSet set = stmt
				.executeQuery("select * from micrblog where mid in (" + inCond
						+ ");");
		while (set.next()) {
			ret.put(set.getLong("id"), set.getString("content"));
		}
		return ret;
	}
}
