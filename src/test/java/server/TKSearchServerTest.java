package server;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Arrays;

import org.junit.Test;

import searchapi.FetchTweetQuery;
import searchapi.Tweets;

public class TKSearchServerTest {
	@Test
	public void test() throws IOException, SQLException {
		TKSearchServer server = new TKSearchServer();
		server.start("", "");
		FetchTweetQuery query = new FetchTweetQuery();
		query.setTids(Arrays.asList(-10908147781l, -10908147781l, -10908147782l, -10908312824011l));
		try {
			Tweets tweets = server.fetchTweets(query);
			System.out.println(tweets);
		} catch (Exception e) {
			e.printStackTrace();
		}

		server.stop();
	}
}
