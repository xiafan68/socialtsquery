/**
 * The first thing to know about are types. The available types in Thrift are:
 *
 *  bool        Boolean, one byte
 *  byte        Signed byte
 *  i16         Signed 16-bit integer
 *  i32         Signed 32-bit integer
 *  i64         Signed 64-bit integer
 *  double      64-bit floating point value
 *  string      String
 *  binary      Blob (byte array)
 *  map<t1,t2>  Map from one type to another
 *  list<t1>    Ordered list of one type
 *  set<t1>     Set of unique elements of one type
 *
 * Did you also notice that Thrift supports C style comments?
 */

// Just in case you were wondering... yes. We support simple C comments too.

/**
 * Thrift files can namespace, package, or prefix their output in various
 * target languages.
 */
namespace java searchapi

struct FetchTweetQuery {
	1: list<i64> tids,
	2: i32 startTime, 
	3: i32 endTime
}

struct TKeywordQuery {
	1: list<string> keywords,
	2: i32 topk,
	3: i32 startTime,
	4: i32 endTime
}

struct TweetTuple{
	1: string content,
	2: list<list<i32>> points
}
struct Tweets{
	1: map<i64, TweetTuple> tweetMap,
}

/**
 * Structs can also be exceptions, if they are nasty.
 */
exception InvalidJob {
  1: i32 what,
  2: string why
}

service TweetService {
	list<i64> search(1:TKeywordQuery query),
   	Tweets fetchTweets(1:FetchTweetQuery query) throws (1:InvalidJob ex),
}
